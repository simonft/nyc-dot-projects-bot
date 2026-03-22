import json
from unittest.mock import MagicMock, patch

import pytest
from bs4 import BeautifulSoup, Tag
from click.testing import CliRunner

from nyc_dot_bot import (
    LocalCache,
    S3Cache,
    TooManyNewPDFsException,
    _default_s3_path,
    cli,
    find_new_links,
    format_link_for_tweet,
    get_pdf_links,
    make_cache,
    parse_s3_path,
    run,
    truncate_text_for_skeet,
)


def make_link(href: str, text: str) -> Tag:
    """Build a bs4 <a> Tag for testing."""
    html = f'<a href="{href}">{text}</a>'
    return BeautifulSoup(html, "html.parser").a


# --- parse_s3_path ---


def test_parse_s3_path_basic():
    assert parse_s3_path("s3://my-bucket/my-key.json") == ("my-bucket", "my-key.json")


def test_parse_s3_path_nested_key():
    assert parse_s3_path("s3://bucket/path/to/key.json") == ("bucket", "path/to/key.json")


def test_parse_s3_path_no_key():
    assert parse_s3_path("s3://bucket/") == ("bucket", "")


# --- make_cache ---


def test_make_cache_returns_local_for_file_path():
    cache = make_cache("/tmp/cache.json")
    assert isinstance(cache, LocalCache)
    assert cache.path == "/tmp/cache.json"


@patch("nyc_dot_bot.boto3")
def test_make_cache_returns_s3_for_s3_url(mock_boto3):
    cache = make_cache("s3://my-bucket/cache.json")
    assert isinstance(cache, S3Cache)
    assert cache.bucket == "my-bucket"
    assert cache.key == "cache.json"


# --- LocalCache ---


def test_local_cache_read_write(tmp_path):
    path = str(tmp_path / "cache.json")
    data = {"https://example.com/a.pdf": "Project A"}

    cache = LocalCache(path)
    cache.write(data)

    result = cache.read()
    assert result == data


def test_local_cache_read_missing_file(tmp_path):
    cache = LocalCache(str(tmp_path / "missing.json"))
    with pytest.raises(FileNotFoundError):
        cache.read()


# --- get_pdf_links ---


def test_get_pdf_links_filters_pdfs():
    html = """
    <html><body>
    <div class="view-content">
        <a href="/doc/a.pdf">PDF A</a>
        <a href="/doc/b.html">HTML B</a>
        <a href="/doc/c.pdf">PDF C</a>
    </div>
    </body></html>
    """
    response = MagicMock()
    response.text = html
    links = get_pdf_links(response)
    assert len(links) == 2
    assert links[0].text == "PDF A"
    assert links[1].text == "PDF C"


def test_get_pdf_links_empty():
    html = '<html><body><div class="view-content"></div></body></html>'
    response = MagicMock()
    response.text = html
    assert get_pdf_links(response) == []


# --- find_new_links ---


def test_find_new_links_returns_only_new():
    cached = {"https://www1.nyc.gov/doc/a.pdf": "A"}
    current = [make_link("/doc/a.pdf", "A"), make_link("/doc/b.pdf", "B")]
    result = find_new_links(cached, current)
    assert len(result) == 1
    assert result[0]["href"] == "https://www1.nyc.gov/doc/b.pdf"


def test_find_new_links_all_cached():
    cached = {
        "https://www1.nyc.gov/doc/a.pdf": "A",
        "https://www1.nyc.gov/doc/b.pdf": "B",
    }
    current = [make_link("/doc/a.pdf", "A"), make_link("/doc/b.pdf", "B")]
    assert find_new_links(cached, current) == []


def test_find_new_links_resolves_relative_urls():
    cached: dict[str, str] = {}
    current = [make_link("/doc/relative.pdf", "Relative")]
    result = find_new_links(cached, current)
    assert result[0]["href"] == "https://www1.nyc.gov/doc/relative.pdf"


def test_find_new_links_too_many_raises():
    cached: dict[str, str] = {}
    current = [make_link(f"/doc/{i}.pdf", f"Link {i}") for i in range(1501)]
    with pytest.raises(TooManyNewPDFsException):
        find_new_links(cached, current)


# --- format_link_for_tweet ---


def test_format_link_for_tweet_short():
    link = make_link("https://example.com/a.pdf", "Project A (pdf)")
    result = format_link_for_tweet(link)
    assert result == "Project A https://example.com/a.pdf"


def test_format_link_for_tweet_truncates_long_text():
    long_text = "A" * 300
    link = make_link("https://example.com/a.pdf", long_text)
    result = format_link_for_tweet(link)
    text_part = result.split(" https://")[0]
    # 280 - 23 (link) - 1 (space) = 256 max, truncated with ...
    assert len(text_part) == 256
    assert text_part.endswith("...")


# --- truncate_text_for_skeet ---


def test_truncate_text_for_skeet_short():
    link = make_link("https://example.com/a.pdf", "Project A (pdf)")
    assert truncate_text_for_skeet(link) == "Project A"


def test_truncate_text_for_skeet_truncates_long_text():
    long_text = "B" * 350
    link = make_link("https://example.com/a.pdf", long_text)
    result = truncate_text_for_skeet(link)
    assert len(result) == 299
    assert result.endswith("...")


# --- _default_s3_path ---


def test_default_s3_path_uses_env(monkeypatch):
    monkeypatch.setenv("BUCKET_NAME", "my-bucket")
    assert _default_s3_path() == "s3://my-bucket/cache.json"


def test_default_s3_path_fallback(monkeypatch):
    monkeypatch.delenv("BUCKET_NAME", raising=False)
    assert _default_s3_path() == "s3://nyc-dot-current-projects-bot-mastodon-staging/cache.json"


# --- run ---


@patch("nyc_dot_bot.get_html")
@patch("nyc_dot_bot.get_pdf_links")
def test_run_no_new_links(mock_get_pdf_links, mock_get_html, tmp_path):
    cache_path = str(tmp_path / "cache.json")
    cache_data = {"https://www1.nyc.gov/doc/a.pdf": "A"}
    with open(cache_path, "w") as f:
        json.dump(cache_data, f)

    link = make_link("/doc/a.pdf", "A")
    mock_get_pdf_links.return_value = [link]

    run(cache_path, dry_run=True)

    # Cache should be unchanged
    with open(cache_path) as f:
        assert json.load(f) == cache_data


@patch("nyc_dot_bot.tweet_new_links")
@patch("nyc_dot_bot.get_html")
@patch("nyc_dot_bot.get_pdf_links")
def test_run_dry_run_does_not_write_cache(mock_get_pdf_links, mock_get_html, mock_tweet, tmp_path):
    cache_path = str(tmp_path / "cache.json")
    with open(cache_path, "w") as f:
        json.dump({}, f)

    link = make_link("/doc/new.pdf", "New")
    mock_get_pdf_links.return_value = [link]
    mock_tweet.return_value = {"https://www1.nyc.gov/doc/new.pdf": "New"}

    run(cache_path, dry_run=True)

    with open(cache_path) as f:
        assert json.load(f) == {}


@patch("nyc_dot_bot.tweet_new_links")
@patch("nyc_dot_bot.get_html")
@patch("nyc_dot_bot.get_pdf_links")
def test_run_writes_successes_to_cache(mock_get_pdf_links, mock_get_html, mock_tweet, tmp_path):
    cache_path = str(tmp_path / "cache.json")
    with open(cache_path, "w") as f:
        json.dump({}, f)

    link = make_link("/doc/new.pdf", "New")
    mock_get_pdf_links.return_value = [link]
    mock_tweet.return_value = {"https://www1.nyc.gov/doc/new.pdf": "New"}

    run(cache_path)

    with open(cache_path) as f:
        result = json.load(f)
    assert result == {"https://www1.nyc.gov/doc/new.pdf": "New"}


# --- cli ---


@patch("nyc_dot_bot.run")
def test_cli_with_cache_flag(mock_run):
    runner = CliRunner()
    result = runner.invoke(cli, ["--cache", "/tmp/test.json", "--dry-run"])
    assert result.exit_code == 0
    mock_run.assert_called_once_with("/tmp/test.json", dry_run=True, no_tweet=False)


@patch("nyc_dot_bot.run")
def test_cli_defaults_to_s3(mock_run, monkeypatch):
    monkeypatch.delenv("BUCKET_NAME", raising=False)
    runner = CliRunner()
    result = runner.invoke(cli, ["--dry-run"])
    assert result.exit_code == 0
    mock_run.assert_called_once_with(
        "s3://nyc-dot-current-projects-bot-mastodon-staging/cache.json",
        dry_run=True,
        no_tweet=False,
    )
