import json
import os

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


def tweet(event=None, context=None):
    projects_html = requests.get(
        "https://www1.nyc.gov/html/dot/html/about/current-projects.shtml"
    )
    projects_html.raise_for_status()
    soup = BeautifulSoup(projects_html.text, "html.parser")
    content = soup.find(class_="region-content")
    links = content.find_all("a")

    pdf_links = []
    for link in links:
        if link["href"].endswith("pdf"):
            pdf_links.append(link)

    client = boto3.client("s3")

    cache = client.get_object(Bucket="nyc-dot-current-projects-bot", Key="cache.json")
    cache = json.loads(cache["Body"].read())

    new_links = []

    for link in pdf_links:
        if link["href"] not in cache:
            new_links.append(link)

    # prevent tweeting too many
    if len(new_links) > 15:
        raise TooManyNewPDFsException

    if not new_links:
        return

    auth = tweepy.OAuthHandler(
        os.environ.get("TWITTER_CONSUMER_KEY"),
        os.environ.get("TWITTER_CONSUMER_SECRET"),
    )
    auth.set_access_token(
        os.environ.get("TWITTER_ACCESS_TOKEN"),
        os.environ.get("TWITTER_ACCESS_TOKEN_SECRET"),
    )
    api = tweepy.API(auth)

    for link in new_links:
        # link takes 23 chars and we want a space
        max_length = 280 - 23 - 1
        link_text = link.text
        link_text = link_text.replace(" (pdf)", "")
        if len(link_text) >= max_length:
            link_text = f"{link_text[max_length-3]}..."

        tweet_text = f"{link_text} {link['href']}"
        print(tweet_text)

        api.update_status(tweet_text)

        cache[link["href"]] = link.text
        client.put_object(
            Bucket="nyc-dot-current-projects-bot",
            Key="cache.json",
            Body=json.dumps(cache),
        )


if __name__ == "__main__":
    tweet()
