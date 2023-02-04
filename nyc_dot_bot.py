import io
import json
from pathlib import Path
import os
import datetime
from urllib.parse import urljoin

import click
import boto3
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from feedgen.feed import FeedGenerator
from mastodon import Mastodon
from pytz import timezone
import requests
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
import tweepy


sentry_sdk.init(
    integrations=[AwsLambdaIntegration()],
    traces_sample_rate=1.0,
)

load_dotenv()

current_projects_url = "https://www1.nyc.gov/html/dot/html/about/current-projects.shtml"

bucket_name = os.environ.get("BUCKET_NAME") or "nyc-dot-current-projects-bot-mastodon"


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
    instructions = {
        "parts": [{"file": "document"}],
        "output": {"type": "image", "format": "png", "width": 1200},
    }

    r = requests.request(
        "POST",
        "https://api.pspdfkit.com/build",
        headers={"Authorization": f"Bearer {os.environ.get('PDF_LIVE_KEY')}"},
        files={"document": pdf},
        data={"instructions": json.dumps(instructions)},
        stream=True,
    )
    r.raise_for_status()
    return r.content


def find_new_links(cached_links, current_links):
    new_links = []
    for link in current_links:
        link["href"] = urljoin(
            "https://www1.nyc.gov/html/dot/html/about/current-projects.shtml",
            link["href"],
        )
        if link["href"] not in cached_links:
            new_links.append(link)

    # prevent tweeting too many
    if len(new_links) > 15:
        raise TooManyNewPDFsException

    return new_links


def format_link_for_tweet(link):
    max_length = 280 - 23 - 1
    link_text = link.text
    link_text = link_text.replace(" (pdf)", "")
    if len(link_text) >= max_length:
        link_text = f"{link_text[max_length-3]}..."

    return f"{link_text} {link['href']}"


def tweet_new_links(links, dry_run=False, no_tweet=False):
    mastodon = Mastodon(api_base_url=os.environ.get("MASTODON_API_BASE_URL"),
                        access_token=os.environ.get("MASTODON_ACCESS_TOKEN"))

    successes = {}

    # If any of these fail, we want to record the rest succeeded so
    # we don't tweet them again. We still want them to go to setry though.
    for link in links:
        try:
            # link takes 23 chars and we want a space
            tweet_text = format_link_for_tweet(link)

            if dry_run or no_tweet:
                print(f'Would have tweeted: "{tweet_text}"')
            else:
                image = convert_pdf_to_image(get_pdf(link["href"]))
                mastodon_media = mastodon.media_post(
                    image,
                    mime_type="image/png",
                    description="Screenshot of first page of PDF. Auto posted so can't describe, sorry."
                )
                mastodon.status_post(tweet_text, media_ids=[mastodon_media["id"]])

            successes[link["href"]] = link.text
        except Exception as e:
            sentry_sdk.capture_exception(e)

    return successes


def update_feed(local_cache, client, dry_run, new_links):
    client = boto3.client("s3")
    feed_json = []
    cache_path = "feed-cache.json"
    if local_cache:
        cache_path = Path(local_cache).parent / cache_path
        try:
            with cache_path.open() as f:
                feed_json = json.loads(f.read())
        except FileNotFoundError:
            pass
    else:
        try:
            feed_json = get_s3_cache(client, key=cache_path)
        # We exepect the path to be there, but it won't the first time.
        except client.exceptions.NoSuchKey as e:
            sentry_sdk.capture_exception(e)

    for link, text in new_links.items():
        feed_json.append(
            {
                "link": link,
                "text": text,
                "time": int(datetime.datetime.now().timestamp()),
            }
        )

    fg = FeedGenerator()
    fg.id(current_projects_url)
    fg.title("NYC DOT New Projects (unofficial)")
    fg.link(href=current_projects_url)
    fg.description("New PDFs posted to the NYC DOT's current projects page")

    rss_links = feed_json[-10:]
    rss_links.reverse()

    for link in rss_links:
        fe = fg.add_entry()
        fe.id(link["link"])
        fe.link(href=link["link"])
        fe.title(link["text"].replace(" (pdf)", ""))
        fe.published(
            datetime.datetime.fromtimestamp(link["time"], tz=timezone("US/Eastern"))
        )

    rss_string = fg.rss_str(pretty=True, encoding="unicode", xml_declaration=False)

    if dry_run:
        return

    if local_cache:
        with cache_path.open("w") as f:
            f.write(json.dumps(feed_json))
        with (Path(local_cache).parent / Path("feed.rss")).open("w") as f:
            f.write(rss_string)
    else:
        client.put_object(
            Bucket=bucket_name,
            Key=cache_path,
            Body=json.dumps(feed_json),
        )
        client.put_object(
            Bucket=bucket_name,
            Key="feed.xml",
            Body=rss_string,
            ACL="public-read",
        )


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

    update_feed(local_cache, client, dry_run, successes)

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
