import io
import random
from unittest.mock import MagicMock, patch
from urllib.parse import urljoin

import pytest
from bs4 import BeautifulSoup, Tag
from click.testing import CliRunner

from nyc_dot_bot import (
    BlueskyPoster,
    CacheData,
    LocalCache,
    MastodonPoster,
    S3Cache,
    TooManyNewPDFsException,
    TwitterPoster,
    _default_s3_path,
    _make_poster,
    cli,
    find_new_links,
    format_link_for_post,
    get_html,
    get_pdf_links,
    make_cache,
    parse_s3_path,
    post_new_links,
    run,
)


def make_link(href: str, text: str) -> Tag:
    """Build a bs4 <a> Tag for testing."""
    html = f'<a href="{href}">{text}</a>'
    tag = BeautifulSoup(html, "html.parser").a
    assert tag is not None
    return tag


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
    data = CacheData(links={"https://example.com/a.pdf": "Project A"})

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
    cached = CacheData(links={"https://www1.nyc.gov/doc/a.pdf": "A"})
    current = [make_link("/doc/a.pdf", "A"), make_link("/doc/b.pdf", "B")]
    result = find_new_links(cached, current)
    assert len(result) == 1
    assert result[0]["href"] == "https://www1.nyc.gov/doc/b.pdf"


def test_find_new_links_all_cached():
    cached = CacheData(
        links={
            "https://www1.nyc.gov/doc/a.pdf": "A",
            "https://www1.nyc.gov/doc/b.pdf": "B",
        }
    )
    current = [make_link("/doc/a.pdf", "A"), make_link("/doc/b.pdf", "B")]
    assert find_new_links(cached, current) == []


def test_find_new_links_resolves_relative_urls():
    cached = CacheData()
    current = [make_link("/doc/relative.pdf", "Relative")]
    result = find_new_links(cached, current)
    assert result[0]["href"] == "https://www1.nyc.gov/doc/relative.pdf"


def test_find_new_links_too_many_raises():
    cached = CacheData()
    current = [make_link(f"/doc/{i}.pdf", f"Link {i}") for i in range(16)]
    with pytest.raises(TooManyNewPDFsException):
        find_new_links(cached, current)


# --- format_link_for_post ---


def test_format_link_for_post_short():
    link = make_link("https://example.com/a.pdf", "Project A (pdf)")
    result = format_link_for_post(link)
    assert result == "Project A https://example.com/a.pdf"


def test_format_link_for_post_normalizes_whitespace():
    link = make_link(
        "https://example.com/a.pdf",
        "Courtlandt\r\n                                          Avenue, Park Avenue, and Morris Avenue"
        " - presented to Bronx Community Board 1\r\n                                          in December 2025 (pdf)",
    )
    result = format_link_for_post(link)
    assert result == (
        "Courtlandt Avenue, Park Avenue, and Morris Avenue"
        " - presented to Bronx Community Board 1 in December 2025 https://example.com/a.pdf"
    )


def test_format_link_for_post_truncates_long_text():
    long_text = "A" * 300
    link = make_link("https://example.com/a.pdf", long_text)
    result = format_link_for_post(link)
    text_part = result.split(" https://")[0]
    # 280 - 23 (link) - 1 (space) = 256 max, truncated with ...
    assert len(text_part) == 256
    assert text_part.endswith("...")


# --- _default_s3_path ---


def test_default_s3_path_uses_env(monkeypatch):
    monkeypatch.setenv("BUCKET_NAME", "my-bucket")
    assert _default_s3_path() == "s3://my-bucket/cache.json"


def test_default_s3_path_fallback(monkeypatch):
    monkeypatch.delenv("BUCKET_NAME", raising=False)
    assert _default_s3_path() == "s3://nyc-dot-current-projects-bot-mastodon-staging/cache.json"


# --- _make_poster ---


@patch("nyc_dot_bot.tweepy")
def test_make_poster_twitter(mock_tweepy, monkeypatch):
    monkeypatch.setenv("TWITTER_CONSUMER_KEY", "key")
    monkeypatch.delenv("BLUESKY_USERNAME", raising=False)
    assert isinstance(_make_poster(), TwitterPoster)


@patch("nyc_dot_bot.Client")
def test_make_poster_bluesky(mock_client, monkeypatch):
    monkeypatch.delenv("TWITTER_CONSUMER_KEY", raising=False)
    monkeypatch.setenv("BLUESKY_USERNAME", "user")
    monkeypatch.setenv("BLUESKY_APP_PASSWORD", "pass")
    assert isinstance(_make_poster(), BlueskyPoster)


@patch("nyc_dot_bot.Mastodon")
def test_make_poster_mastodon(mock_mastodon, monkeypatch):
    monkeypatch.delenv("TWITTER_CONSUMER_KEY", raising=False)
    monkeypatch.delenv("BLUESKY_USERNAME", raising=False)
    monkeypatch.setenv("MASTODON_ACCESS_TOKEN", "token")
    assert isinstance(_make_poster(), MastodonPoster)


def test_make_poster_no_credentials_raises(monkeypatch):
    monkeypatch.delenv("TWITTER_CONSUMER_KEY", raising=False)
    monkeypatch.delenv("BLUESKY_USERNAME", raising=False)
    monkeypatch.delenv("MASTODON_ACCESS_TOKEN", raising=False)
    with pytest.raises(ValueError, match="No platform credentials configured"):
        _make_poster()


# --- post_new_links ---


@patch("nyc_dot_bot.convert_pdf_to_image")
@patch("nyc_dot_bot.get_pdf")
def test_post_new_links_no_poster_prints(mock_get_pdf, mock_convert, capsys):
    link = make_link("https://example.com/a.pdf", "Project A (pdf)")
    mock_get_pdf.return_value = b"pdf"
    mock_convert.return_value = io.BytesIO(b"jpegdata")

    result = post_new_links([link])
    assert result == {"https://example.com/a.pdf": "Project A (pdf)"}
    assert 'Would have posted: "Project A https://example.com/a.pdf"' in capsys.readouterr().out


@patch("nyc_dot_bot.convert_pdf_to_image")
@patch("nyc_dot_bot.get_pdf")
def test_post_new_links_with_poster_calls_post(mock_get_pdf, mock_convert):
    link = make_link("https://example.com/a.pdf", "Project A")
    mock_get_pdf.return_value = b"pdf"
    mock_convert.return_value = io.BytesIO(b"jpegdata")

    poster = MagicMock()

    result = post_new_links([link], poster)
    assert result == {"https://example.com/a.pdf": "Project A"}
    poster.post.assert_called_once()
    posted_link, posted_title, posted_image = poster.post.call_args[0]
    assert posted_link["href"] == "https://example.com/a.pdf"
    assert posted_title == "Project A"
    assert posted_image == b"jpegdata"


@patch("nyc_dot_bot.convert_pdf_to_image")
@patch("nyc_dot_bot.get_pdf")
def test_post_new_links_continues_after_failure(mock_get_pdf, mock_convert):
    link1 = make_link("https://example.com/a.pdf", "A")
    link2 = make_link("https://example.com/b.pdf", "B")
    mock_get_pdf.return_value = b"pdf"
    mock_convert.return_value = io.BytesIO(b"jpegdata")

    poster = MagicMock()
    poster.post.side_effect = [RuntimeError("fail"), None]

    result = post_new_links([link1, link2], poster)
    assert result == {"https://example.com/b.pdf": "B"}
    assert poster.post.call_count == 2


# --- run ---


@patch("nyc_dot_bot.get_html")
@patch("nyc_dot_bot.get_pdf_links")
def test_run_no_new_links(mock_get_pdf_links, mock_get_html, tmp_path):
    cache_path = str(tmp_path / "cache.json")
    initial = CacheData(links={"https://www1.nyc.gov/doc/a.pdf": "A"})
    LocalCache(cache_path).write(initial)

    link = make_link("/doc/a.pdf", "A")
    mock_get_pdf_links.return_value = [link]

    run(cache_path, dry_run=True)

    # Cache should be unchanged
    assert LocalCache(cache_path).read() == initial


@patch("nyc_dot_bot.post_new_links")
@patch("nyc_dot_bot.get_html")
@patch("nyc_dot_bot.get_pdf_links")
def test_run_dry_run_does_not_write_cache(mock_get_pdf_links, mock_get_html, mock_post, tmp_path):
    cache_path = str(tmp_path / "cache.json")
    initial = CacheData()
    LocalCache(cache_path).write(initial)

    link = make_link("/doc/new.pdf", "New")
    mock_get_pdf_links.return_value = [link]
    mock_post.return_value = {"https://www1.nyc.gov/doc/new.pdf": "New"}

    run(cache_path, dry_run=True)

    assert LocalCache(cache_path).read() == initial


@patch("nyc_dot_bot._make_poster")
@patch("nyc_dot_bot.post_new_links")
@patch("nyc_dot_bot.get_html")
@patch("nyc_dot_bot.get_pdf_links")
def test_run_writes_successes_to_cache(mock_get_pdf_links, mock_get_html, mock_post, mock_make_poster, tmp_path):
    cache_path = str(tmp_path / "cache.json")
    LocalCache(cache_path).write(CacheData())

    link = make_link("/doc/new.pdf", "New")
    mock_get_pdf_links.return_value = [link]
    mock_post.return_value = {"https://www1.nyc.gov/doc/new.pdf": "New"}

    run(cache_path)

    result = LocalCache(cache_path).read()
    assert result == CacheData(links={"https://www1.nyc.gov/doc/new.pdf": "New"})


@patch("nyc_dot_bot._make_poster")
@patch("nyc_dot_bot.post_new_links")
@patch("nyc_dot_bot.get_html")
@patch("nyc_dot_bot.get_pdf_links")
def test_run_merges_new_links_with_existing_cache(mock_get_pdf_links, mock_get_html, mock_post, mock_make_poster, tmp_path):
    cache_path = str(tmp_path / "cache.json")
    LocalCache(cache_path).write(CacheData(links={"https://www1.nyc.gov/doc/old.pdf": "Old"}))

    mock_get_pdf_links.return_value = [make_link("/doc/old.pdf", "Old"), make_link("/doc/new.pdf", "New")]
    mock_post.return_value = {"https://www1.nyc.gov/doc/new.pdf": "New"}

    run(cache_path)

    result = LocalCache(cache_path).read()
    assert result == CacheData(
        links={
            "https://www1.nyc.gov/doc/old.pdf": "Old",
            "https://www1.nyc.gov/doc/new.pdf": "New",
        }
    )


@patch("nyc_dot_bot.post_new_links")
@patch("nyc_dot_bot.get_html")
@patch("nyc_dot_bot.get_pdf_links")
def test_run_no_post_writes_cache(mock_get_pdf_links, mock_get_html, mock_post, tmp_path):
    cache_path = str(tmp_path / "cache.json")
    LocalCache(cache_path).write(CacheData())

    mock_get_pdf_links.return_value = [make_link("/doc/new.pdf", "New")]
    mock_post.return_value = {"https://www1.nyc.gov/doc/new.pdf": "New"}

    run(cache_path, no_post=True)

    result = LocalCache(cache_path).read()
    assert result == CacheData(links={"https://www1.nyc.gov/doc/new.pdf": "New"})


# --- cli ---


@patch("nyc_dot_bot.load_dotenv")
@patch("nyc_dot_bot.run")
def test_cli_post_with_cache_flag(mock_run, _mock_dotenv):
    runner = CliRunner()
    result = runner.invoke(cli, ["post", "--cache", "/tmp/test.json", "--dry-run"])
    assert result.exit_code == 0
    mock_run.assert_called_once_with("/tmp/test.json", dry_run=True, no_post=False)


@patch("nyc_dot_bot.load_dotenv")
@patch("nyc_dot_bot.run")
def test_cli_post_defaults_to_s3(mock_run, _mock_dotenv, monkeypatch):
    monkeypatch.delenv("BUCKET_NAME", raising=False)
    runner = CliRunner()
    result = runner.invoke(cli, ["post", "--dry-run"])
    assert result.exit_code == 0
    mock_run.assert_called_once_with(
        "s3://nyc-dot-current-projects-bot-mastodon-staging/cache.json",
        dry_run=True,
        no_post=False,
    )


@patch("nyc_dot_bot.get_html")
@patch("nyc_dot_bot.get_pdf_links")
def test_cli_prune_dry_run(mock_get_pdf_links, mock_get_html, tmp_path):
    cache_path = str(tmp_path / "cache.json")
    initial = CacheData(
        links={
            "https://www1.nyc.gov/doc/a.pdf": "A",
            "https://www1.nyc.gov/doc/b.pdf": "B",
        }
    )
    LocalCache(cache_path).write(initial)

    # Only "a.pdf" is still on the page
    mock_get_pdf_links.return_value = [make_link("/doc/a.pdf", "A")]

    runner = CliRunner()
    result = runner.invoke(cli, ["prune", "--cache", cache_path, "--dry-run"])
    assert result.exit_code == 0
    assert "Removing: B" in result.output

    # Cache should be unchanged in dry-run
    assert LocalCache(cache_path).read() == initial


@patch("nyc_dot_bot.get_html")
@patch("nyc_dot_bot.get_pdf_links")
def test_cli_prune_removes_stale(mock_get_pdf_links, mock_get_html, tmp_path):
    cache_path = str(tmp_path / "cache.json")
    LocalCache(cache_path).write(
        CacheData(
            links={
                "https://www1.nyc.gov/doc/a.pdf": "A",
                "https://www1.nyc.gov/doc/b.pdf": "B",
            }
        )
    )

    mock_get_pdf_links.return_value = [make_link("/doc/a.pdf", "A")]

    runner = CliRunner()
    result = runner.invoke(cli, ["prune", "--cache", cache_path])
    assert result.exit_code == 0

    result_cache = LocalCache(cache_path).read()
    assert result_cache == CacheData(links={"https://www1.nyc.gov/doc/a.pdf": "A"})


# --- integration ---


@pytest.mark.integration
def test_detects_removed_link_from_cache(tmp_path):
    html = get_html()
    all_links = get_pdf_links(html)
    assert len(all_links) > 0, "Expected at least one PDF link on the page"

    # Build a full cache from all current links
    cached = CacheData()
    for link in all_links:
        resolved = urljoin("https://www1.nyc.gov/html/dot/html/about/current-projects.shtml", str(link["href"]))
        cached.links[resolved] = link.text

    # Remove one link at random
    removed_url = random.choice(list(cached.links.keys()))
    cached.links.pop(removed_url)

    # Re-fetch and find new links against the modified cache
    html = get_html()
    new_links = find_new_links(cached, get_pdf_links(html))

    new_urls = [link["href"] for link in new_links]
    assert removed_url in new_urls
    # All new links should point to the one URL we removed
    assert all(url == removed_url for url in new_urls)
