import copy
import io
import json
import os
import traceback
from typing import Any, Protocol
from urllib.parse import urljoin

import boto3
import click
import requests
import sentry_sdk
import tweepy
from atproto import Client, client_utils
from bs4 import BeautifulSoup, Tag
from dotenv import load_dotenv
from mastodon import Mastodon
from pdf2image import convert_from_bytes
from pydantic import BaseModel

current_projects_url = "https://www1.nyc.gov/html/dot/html/about/current-projects.shtml"

DEFAULT_BUCKET = "nyc-dot-current-projects-bot-mastodon-staging"


def parse_s3_path(path: str) -> tuple[str, str]:
    """Split 's3://bucket/key' into (bucket, key)."""
    without_scheme = path[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


class CacheData(BaseModel):
    links: dict[str, str] = {}

    @classmethod
    def from_json(cls, raw: str | bytes) -> "CacheData":
        """Parse JSON, handling both new format {"links": {...}} and legacy flat format {...}."""
        data = json.loads(raw)
        if "links" in data:
            return cls.model_validate(data)
        return cls(links=data)


class Cache(Protocol):
    def read(self) -> CacheData: ...
    def write(self, data: CacheData) -> None: ...


class LocalCache(Cache):
    def __init__(self, path: str) -> None:
        self.path = path

    def read(self) -> CacheData:
        with open(self.path) as f:
            return CacheData.from_json(f.read())

    def write(self, data: CacheData) -> None:
        with open(self.path, "w") as f:
            f.write(data.model_dump_json())


class S3Cache(Cache):
    def __init__(self, bucket: str, key: str) -> None:
        self.bucket = bucket
        self.key = key
        self.client: Any = boto3.client("s3")

    def read(self) -> CacheData:
        obj = self.client.get_object(Bucket=self.bucket, Key=self.key)
        return CacheData.from_json(obj["Body"].read())

    def write(self, data: CacheData) -> None:
        self.client.put_object(
            Bucket=self.bucket,
            Key=self.key,
            Body=data.model_dump_json(),
        )


def make_cache(cache_path: str) -> Cache:
    if cache_path.startswith("s3://"):
        bucket, key = parse_s3_path(cache_path)
        return S3Cache(bucket, key)
    return LocalCache(cache_path)


class PlatformPoster(Protocol):
    def post(self, link: Tag, image_buf: io.BytesIO) -> None: ...


class TwitterPoster(PlatformPoster):
    def __init__(self) -> None:
        auth = tweepy.OAuth1UserHandler(
            os.environ.get("TWITTER_CONSUMER_KEY"),
            os.environ.get("TWITTER_CONSUMER_SECRET"),
            os.environ.get("TWITTER_ACCESS_TOKEN"),
            os.environ.get("TWITTER_ACCESS_TOKEN_SECRET"),
        )
        self.client_v1 = tweepy.API(auth)
        self.client_v2 = tweepy.Client(
            consumer_key=os.environ.get("TWITTER_CONSUMER_KEY"),
            consumer_secret=os.environ.get("TWITTER_CONSUMER_SECRET"),
            access_token=os.environ.get("TWITTER_ACCESS_TOKEN"),
            access_token_secret=os.environ.get("TWITTER_ACCESS_TOKEN_SECRET"),
        )

    def post(self, link: Tag, image_buf: io.BytesIO) -> None:
        post_text = format_link_for_post(link)
        media = self.client_v1.media_upload(filename="", file=image_buf)
        self.client_v2.create_tweet(text=post_text, media_ids=[media.media_id])


class BlueskyPoster(PlatformPoster):
    def __init__(self) -> None:
        self.client = Client()
        self.client.login(os.environ.get("BLUESKY_USERNAME"), os.environ.get("BLUESKY_APP_PASSWORD"))

    def post(self, link: Tag, image_buf: io.BytesIO) -> None:
        self.client.send_image(
            text=client_utils.TextBuilder().link(
                truncate_text_for_skeet(link),
                str(link["href"]),
            ),
            image=image_buf.read(),
            image_alt="Screenshot of first page of PDF. Auto posted so can't describe, sorry.",
        )


class MastodonPoster(PlatformPoster):
    def __init__(self) -> None:
        self.client = Mastodon(
            api_base_url=os.environ.get("MASTODON_API_BASE_URL"),
            access_token=os.environ.get("MASTODON_ACCESS_TOKEN"),
        )

    def post(self, link: Tag, image_buf: io.BytesIO) -> None:
        post_text = format_link_for_post(link)
        image = image_buf.read()
        mastodon_media = self.client.media_post(
            image,
            mime_type="image/jpeg",
            description="Screenshot of first page of PDF. Auto posted so can't describe, sorry.",
        )
        self.client.status_post(post_text, media_ids=[mastodon_media["id"]])


def _make_poster() -> PlatformPoster:
    if os.environ.get("TWITTER_CONSUMER_KEY"):
        return TwitterPoster()
    elif os.environ.get("BLUESKY_USERNAME"):
        return BlueskyPoster()
    return MastodonPoster()


class TooManyNewPDFsException(Exception):
    pass


def get_html() -> requests.Response:
    projects_html = requests.get(current_projects_url, timeout=30)
    projects_html.raise_for_status()
    projects_html.encoding = "utf-8"
    return projects_html


def get_pdf_links(projects_html: requests.Response) -> list[Tag]:
    soup = BeautifulSoup(projects_html.text, "html.parser")
    content = soup.find(class_="view-content")
    if content is None:
        raise ValueError("Could not find 'view-content' element on page")
    links = content.find_all("a")

    pdf_links: list[Tag] = []
    for link in links:
        if str(link["href"]).endswith("pdf"):
            pdf_links.append(link)
    return pdf_links


def get_pdf(link: str) -> bytes:
    r = requests.get(link, timeout=30)
    r.raise_for_status()
    return r.content


def convert_pdf_to_image(pdf: bytes) -> io.BytesIO:
    buf = io.BytesIO()
    image = convert_from_bytes(pdf)[0]
    image.thumbnail((2048, 2048))
    image.save(buf, format="JPEG")
    buf.seek(0)
    return buf


def find_new_links(cached: CacheData, current_links: list[Tag]) -> list[Tag]:
    new_links: list[Tag] = []
    for link in current_links:
        resolved = urljoin(current_projects_url, str(link["href"]))
        if resolved not in cached.links:
            new_link = copy.copy(link)
            new_link["href"] = resolved
            new_links.append(new_link)

    # prevent posting too many
    if len(new_links) > 15:
        raise TooManyNewPDFsException

    return new_links


def _clean_link_text(link: Tag) -> str:
    text = " ".join(link.text.split())
    return text.replace(" (pdf)", "")


def format_link_for_post(link: Tag) -> str:
    max_length = 280 - 23 - 1
    link_text = _clean_link_text(link)
    if len(link_text) >= max_length:
        link_text = f"{link_text[: max_length - 3]}..."

    return f"{link_text} {str(link['href'])}"


def truncate_text_for_skeet(link: Tag) -> str:
    max_length = 300 - 1
    link_text = _clean_link_text(link)
    if len(link_text) >= max_length:
        link_text = f"{link_text[: max_length - 3]}..."

    return link_text


def post_new_links(links: list[Tag], poster: PlatformPoster | None = None) -> dict[str, str]:
    successes: dict[str, str] = {}

    # If any of these fail, we want to record the rest succeeded so
    # we don't post them again. We still want them to go to sentry though.
    for link in links:
        try:
            href = str(link["href"])
            image_buf = convert_pdf_to_image(get_pdf(href))

            if poster is None:
                post_text = format_link_for_post(link)
                print(f'Would have posted: "{post_text}"')
            else:
                poster.post(link, image_buf)

            successes[href] = link.text
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            sentry_sdk.capture_exception(e)
            print(e)
            traceback.print_exc()

    return successes


def run(cache_path: str, dry_run: bool = False, no_post: bool = False) -> None:
    cache = make_cache(cache_path)
    cached = cache.read()

    new_links = find_new_links(cached, get_pdf_links(get_html()))

    if not new_links:
        return

    poster = None if (dry_run or no_post) else _make_poster()
    successes = post_new_links(new_links, poster)

    if dry_run:
        return

    cached.links.update(successes)
    cache.write(cached)


def _default_s3_path() -> str:
    bucket = os.environ.get("BUCKET_NAME") or DEFAULT_BUCKET
    return f"s3://{bucket}/cache.json"


def _resolve_cache(cache: str | None) -> str:
    return cache if cache is not None else _default_s3_path()


@click.group()
def cli() -> None:
    load_dotenv()
    sentry_sdk.init()


@cli.command()
@click.option("--dry-run", is_flag=True)
@click.option("--no-post", is_flag=True, help="Updates the cache without posting")
@click.option("--cache", default=None, type=str, help="Cache path (local file or s3://bucket/key)")
def post(dry_run: bool, cache: str | None, no_post: bool) -> None:
    run(_resolve_cache(cache), dry_run=dry_run, no_post=no_post)


@cli.command()
@click.option("--dry-run", is_flag=True)
@click.option("--cache", default=None, type=str, help="Cache path (local file or s3://bucket/key)")
def prune(dry_run: bool, cache: str | None) -> None:
    """Remove cached links that are no longer on the page."""
    cache_obj = make_cache(_resolve_cache(cache))
    cached = cache_obj.read()

    current_links = get_pdf_links(get_html())
    current_urls = set()
    for link in current_links:
        current_urls.add(urljoin(current_projects_url, str(link["href"])))

    stale = {url: text for url, text in cached.links.items() if url not in current_urls}

    if not stale:
        print("No stale links found.")
        return

    for url, text in stale.items():
        print(f"Removing: {text} ({url})")

    if dry_run:
        return

    for url in stale:
        del cached.links[url]
    cache_obj.write(cached)
