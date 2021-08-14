import json
import os

import click
import boto3
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
import tweepy


sentry_sdk.init(
    integrations=[AwsLambdaIntegration()],
    traces_sample_rate=1.0,  # adjust the sample rate in production as needed
)

load_dotenv()


class TooManyNewPDFsException(Exception):
    pass


def get_html():
    projects_html = requests.get(
        "https://www1.nyc.gov/html/dot/html/about/current-projects.shtml"
    )
    projects_html.raise_for_status()
    return projects_html


def get_pdf_links(projects_html):
    soup = BeautifulSoup(projects_html.text, "html.parser")
    content = soup.find(class_="region-content")
    links = content.find_all("a")

    pdf_links = []
    for link in links:
        if link["href"].endswith("pdf"):
            pdf_links.append(link)
    return pdf_links


def get_s3_cache(client):
    cache = client.get_object(Bucket="nyc-dot-current-projects-bot", Key="cache.json")
    return json.loads(cache["Body"].read())


def get_local_cache(file_path):
    with open(file_path) as f:
        content = f.read()
        return json.loads(content)


def find_new_links(cached_links, current_links):
    new_links = []
    for link in current_links:
        if link["href"] not in cached_links:
            new_links.append(link)
            print(link)

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


def tweet_new_links(links, dry_run=False):
    auth = tweepy.OAuthHandler(
        os.environ.get("TWITTER_CONSUMER_KEY"),
        os.environ.get("TWITTER_CONSUMER_SECRET"),
    )
    auth.set_access_token(
        os.environ.get("TWITTER_ACCESS_TOKEN"),
        os.environ.get("TWITTER_ACCESS_TOKEN_SECRET"),
    )
    api = tweepy.API(auth)

    successes = []

    # If any of these fail, we want to record the rest succeeded so
    # we don't tweet them again. We still want them to go to setry though.
    try:
        for link in links:
            # link takes 23 chars and we want a space
            tweet_text = format_link_for_tweet(link)

            if dry_run:
                print(tweet_text)
            else:
                api.update_status(tweet_text)
            successes.append(link)
    except Exception as e:
        sentry_sdk.capture_exception(e)

    return successes


def run(event=None, context=None, local_cache=None, dry_run=False):
    cache = None
    client = None

    if local_cache:
        cache = get_local_cache(local_cache)
    else:
        client = boto3.client("s3")
        cache = get_s3_cache()

    new_links = find_new_links(cache, get_pdf_links(get_html()))

    # print(new_links)
    if not new_links:
        return

    successes = tweet_new_links(new_links, dry_run)

    if dry_run:
        return

    cache.update(successes)
    if local_cache:
        with open(local_cache, "rw") as f:
            f.write(json.dumps(cache))
    else:
        client.put_object(
            Bucket="nyc-dot-current-projects-bot",
            Key="cache.json",
            Body=json.dumps(cache),
        )


@click.command()
@click.option("--dry-run", is_flag=True)
@click.option(
    "--local-cache", default=None, type=click.Path(dir_okay=False, writable=True)
)
def cli(dry_run, local_cache):
    run(local_cache=local_cache, dry_run=dry_run)


if __name__ == "__main__":
    cli()
