import io
import json
import os
import traceback
from enum import Enum, auto
from typing import Any
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

sentry_sdk.init()

load_dotenv()

current_projects_url = "https://www1.nyc.gov/html/dot/html/about/current-projects.shtml"

DEFAULT_BUCKET = "nyc-dot-current-projects-bot-mastodon-staging"


def parse_s3_path(path: str) -> tuple[str, str]:
    """Split 's3://bucket/key' into (bucket, key)."""
    without_scheme = path[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


class LocalCache:
    def __init__(self, path: str) -> None:
        self.path = path

    def read(self) -> dict[str, str]:
        with open(self.path) as f:
            return json.loads(f.read())

    def write(self, data: dict[str, str]) -> None:
        with open(self.path, "w") as f:
            f.write(json.dumps(data))


class S3Cache:
    def __init__(self, bucket: str, key: str) -> None:
        self.bucket = bucket
        self.key = key
        self.client: Any = boto3.client("s3")

    def read(self) -> dict[str, str]:
        obj = self.client.get_object(Bucket=self.bucket, Key=self.key)
        return json.loads(obj["Body"].read())

    def write(self, data: dict[str, str]) -> None:
        self.client.put_object(
            Bucket=self.bucket,
            Key=self.key,
            Body=json.dumps(data),
        )


def make_cache(cache_path: str) -> LocalCache | S3Cache:
    if cache_path.startswith("s3://"):
        bucket, key = parse_s3_path(cache_path)
        return S3Cache(bucket, key)
    return LocalCache(cache_path)


class Platform(Enum):
    MASTODON = auto()
    TWITTER = auto()
    BLUESKY = auto()


def _get_platform() -> Platform:
    if os.environ.get("TWITTER_CONSUMER_KEY"):
        return Platform.TWITTER
    elif os.environ.get("BLUESKY_USERNAME"):
        return Platform.BLUESKY
    return Platform.MASTODON


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
    links = content.find_all("a")

    pdf_links: list[Tag] = []
    for link in links:
        if link["href"].endswith("pdf"):
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


def find_new_links(cached_links: dict[str, str], current_links: list[Tag]) -> list[Tag]:
    new_links: list[Tag] = []
    for link in current_links:
        resolved = urljoin(current_projects_url, link["href"])
        if resolved not in cached_links:
            link["href"] = resolved
            new_links.append(link)

    # prevent tweeting too many
    if len(new_links) > 1500:
        raise TooManyNewPDFsException

    return new_links


def format_link_for_tweet(link: Tag) -> str:
    max_length = 280 - 23 - 1
    link_text = link.text
    link_text = link_text.replace(" (pdf)", "")
    if len(link_text) >= max_length:
        link_text = f"{link_text[: max_length - 3]}..."

    return f"{link_text} {link['href']}"


def truncate_text_for_skeet(link: Tag) -> str:
    max_length = 300 - 1
    link_text = link.text
    link_text = link_text.replace(" (pdf)", "")
    if len(link_text) >= max_length:
        link_text = f"{link_text[: max_length - 3]}..."

    return link_text


def tweet_new_links(links: list[Tag], platform: Platform, dry_run: bool = False, no_tweet: bool = False) -> dict[str, str]:
    successes: dict[str, str] = {}

    if platform is Platform.TWITTER:
        auth = tweepy.OAuth1UserHandler(
            os.environ.get("TWITTER_CONSUMER_KEY"),
            os.environ.get("TWITTER_CONSUMER_SECRET"),
            os.environ.get("TWITTER_ACCESS_TOKEN"),
            os.environ.get("TWITTER_ACCESS_TOKEN_SECRET"),
        )
        twitter_client_v1 = tweepy.API(auth)
        twitter_client_v2 = tweepy.Client(
            consumer_key=os.environ.get("TWITTER_CONSUMER_KEY"),
            consumer_secret=os.environ.get("TWITTER_CONSUMER_SECRET"),
            access_token=os.environ.get("TWITTER_ACCESS_TOKEN"),
            access_token_secret=os.environ.get("TWITTER_ACCESS_TOKEN_SECRET"),
        )
    elif platform is Platform.BLUESKY:
        bsky_client = Client()
        bsky_client.login(os.environ.get("BLUESKY_USERNAME"), os.environ.get("BLUESKY_APP_PASSWORD"))
    else:
        mastodon_client = Mastodon(
            api_base_url=os.environ.get("MASTODON_API_BASE_URL"),
            access_token=os.environ.get("MASTODON_ACCESS_TOKEN"),
        )

    # If any of these fail, we want to record the rest succeeded so
    # we don't tweet them again. We still want them to go to sentry though.
    for link in links:
        try:
            # link takes 23 chars and we want a space
            tweet_text = format_link_for_tweet(link)
            image_buf = convert_pdf_to_image(get_pdf(link["href"]))

            if dry_run or no_tweet:
                print(f'Would have tweeted: "{tweet_text}"')
            else:
                if platform is Platform.TWITTER:
                    media = twitter_client_v1.media_upload(filename="", file=image_buf)
                    twitter_client_v2.create_tweet(text=tweet_text, media_ids=[media.media_id])
                elif platform is Platform.BLUESKY:
                    image = image_buf.read()

                    bsky_client.send_image(
                        text=client_utils.TextBuilder().link(
                            truncate_text_for_skeet(link),
                            link["href"],
                        ),
                        image=image,
                        image_alt="Screenshot of first page of PDF. Auto posted so can't describe, sorry.",
                    )
                else:
                    image = image_buf.read()
                    mastodon_media = mastodon_client.media_post(
                        image,
                        mime_type="image/jpeg",
                        description="Screenshot of first page of PDF. Auto posted so can't describe, sorry.",
                    )
                    mastodon_client.status_post(tweet_text, media_ids=[mastodon_media["id"]])

            successes[link["href"]] = link.text
        except Exception as e:
            sentry_sdk.capture_exception(e)
            print(e)
            traceback.print_exc()

    return successes


def run(cache_path: str, dry_run: bool = False, no_tweet: bool = False) -> None:
    cache = make_cache(cache_path)
    cached_links = cache.read()

    new_links = find_new_links(cached_links, get_pdf_links(get_html()))

    if not new_links:
        return

    successes = tweet_new_links(new_links, _get_platform(), dry_run, no_tweet)

    if dry_run:
        return

    cached_links.update(successes)
    cache.write(cached_links)


def _default_s3_path() -> str:
    bucket = os.environ.get("BUCKET_NAME") or DEFAULT_BUCKET
    return f"s3://{bucket}/cache.json"


def lambda_handler(event: Any = None, context: Any = None) -> None:
    run(_default_s3_path())


@click.command()
@click.option("--dry-run", is_flag=True)
@click.option("--no-tweet", is_flag=True, help="Updates the cache without tweeting")
@click.option("--cache", default=None, type=str, help="Cache path (local file or s3://bucket/key)")
def cli(dry_run: bool, cache: str | None, no_tweet: bool) -> None:
    if cache is None:
        cache = _default_s3_path()
    run(cache, dry_run=dry_run, no_tweet=no_tweet)


if __name__ == "__main__":
    cli()
