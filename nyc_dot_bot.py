from enum import Enum, auto
import io
import json
import os
import traceback
from urllib.parse import urljoin

from atproto import Client, client_utils
import click
import boto3
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from mastodon import Mastodon
from pdf2image import convert_from_bytes
import requests
import sentry_sdk
import tweepy


sentry_sdk.init(
    traces_sample_rate=1.0,
)

load_dotenv()

current_projects_url = "https://www1.nyc.gov/html/dot/html/about/current-projects.shtml"

bucket_name = (
    os.environ.get("BUCKET_NAME") or "nyc-dot-current-projects-bot-mastodon-staging"
)


class Platform(Enum):
    MASTODON = auto()
    TWITTER = auto()
    BLUESKY = auto()


PLATFORM = Platform.MASTODON

if os.environ.get("TWITTER_CONSUMER_KEY"):
    PLATFORM = Platform.TWITTER
elif os.environ.get("BLUESKY_USERNAME"):
    PLATFORM = Platform.BLUESKY


class TooManyNewPDFsException(Exception):
    pass


def get_html():
    projects_html = requests.get(current_projects_url)
    projects_html.raise_for_status()
    projects_html.encoding = "utf-8"
    return projects_html


def get_pdf_links(projects_html):
    soup = BeautifulSoup(projects_html.text, "html.parser")
    content = soup.find(class_="view-content")
    links = content.find_all("a")

    pdf_links = []
    for link in links:
        if link["href"].endswith("pdf"):
            pdf_links.append(link)
    return pdf_links


def get_s3_cache(client, key="cache.json"):
    cache = client.get_object(Bucket=bucket_name, Key=key)
    return json.loads(cache["Body"].read())


def get_local_cache(file_path):
    with open(file_path) as f:
        content = f.read()
        return json.loads(content)


def get_pdf(link):
    r = requests.get(link)
    r.raise_for_status()
    return r.content


def convert_pdf_to_image(pdf):
    buf = io.BytesIO()
    image = convert_from_bytes(pdf)[0]
    image.thumbnail((2048,2048))
    image.save(buf, format="JPEG")
    buf.seek(0)
    return buf


def find_new_links(cached_links, current_links):
    new_links = []
    for link in current_links:
        link["href"] = urljoin(
            current_projects_url,
            link["href"],
        )
        if link["href"] not in cached_links:
            new_links.append(link)

    # prevent tweeting too many
    if len(new_links) > 1500:
        raise TooManyNewPDFsException

    return new_links


def format_link_for_tweet(link):
    max_length = 280 - 23 - 1
    link_text = link.text
    link_text = link_text.replace(" (pdf)", "")
    if len(link_text) >= max_length:
        link_text = f"{link_text[max_length-3]}..."

    return f"{link_text} {link['href']}"

def truncate_text_for_skeet(link):
    max_length = 300 - 1
    link_text = link.text
    link_text = link_text.replace(" (pdf)", "")
    if len(link_text) >= max_length:
        link_text = f"{link_text[max_length-3]}..."

    return link_text


def tweet_new_links(links, dry_run=False, no_tweet=False):
    successes = {}

    if PLATFORM is Platform.TWITTER:
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
    elif PLATFORM is Platform.BLUESKY:
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
                if PLATFORM is Platform.TWITTER:
                    media = twitter_client_v1.media_upload(filename="", file=image_buf)
                    twitter_client_v2.create_tweet(text=tweet_text, media_ids=[media.media_id])
                elif PLATFORM is Platform.BLUESKY:
                    image = image_buf.read()

                    bsky_client.send_image(
                        text=client_utils.TextBuilder().link(
                            truncate_text_for_skeet(link),
                            link['href'],
                        ),
                        image=image,
                        image_alt="Screenshot of first page of PDF. Auto posted so can't describe, sorry."
                    )
                else:
                    image = image_buf.read()
                    mastodon_media = mastodon_client.media_post(
                        image,
                        mime_type="image/png",
                        description="Screenshot of first page of PDF. Auto posted so can't describe, sorry.",
                    )
                    mastodon_client.status_post(tweet_text, media_ids=[mastodon_media["id"]])

            successes[link["href"]] = link.text
        except Exception as e:
            sentry_sdk.capture_exception(e)
            print(e)
            traceback.print_exc()

    return successes


def run(event=None, context=None, local_cache=None, dry_run=False, no_tweet=False):
    cache = None
    client = None

    if local_cache:
        cache = get_local_cache(local_cache)
    else:
        client = boto3.client("s3")
        cache = get_s3_cache(client)

    new_links = find_new_links(cache, get_pdf_links(get_html()))

    if not new_links:
        return

    successes = tweet_new_links(new_links, dry_run, no_tweet)

    if dry_run:
        return

    cache.update(successes)
    if local_cache:
        with open(local_cache, "w") as f:
            f.write(json.dumps(cache))
    else:
        client.put_object(
            Bucket=bucket_name,
            Key="cache.json",
            Body=json.dumps(cache),
        )


def lambda_handler(event=None, context=None):
    run()


@click.command()
@click.option("--dry-run", is_flag=True)
@click.option("--no-tweet", is_flag=True, help="Updates the cache without tweeting")
@click.option(
    "--local-cache", default=None, type=click.Path(dir_okay=False, writable=True)
)
def cli(dry_run, local_cache, no_tweet):
    run(local_cache=local_cache, dry_run=dry_run, no_tweet=no_tweet)


if __name__ == "__main__":
    cli()
