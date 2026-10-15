import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reddit_reports import AccessError, NoRedirects, collect, parse_utc


def post(identifier, timestamp, **extra):
    return {"data": {"name": identifier, "created_utc": timestamp,
                     "permalink": "/r/elasticsearch/comments/" + identifier,
                     "title": "A report", "selftext": "Reproduction", **extra}}


class Pages:
    def __init__(self, pages):
        self.pages = iter(pages)
        self.calls = []

    def get(self, path, params):
        self.calls.append((path, params))
        return {"data": next(self.pages)}


class CollectionTests(unittest.TestCase):
    def collect(self, pages, limit=100):
        return collect(pages, "elasticsearch", datetime.fromtimestamp(100, timezone.utc),
                       datetime.fromtimestamp(200, timezone.utc), limit)

    def test_pages_exact_boundaries_duplicates_removed_and_sticky(self):
        api = Pages([
            {"children": [post("sticky", 1, stickied=True), post("future", 200), post("a", 199)], "after": "p1"},
            {"children": [post("a", 199), post("b", 100), post("deleted", 110, selftext="[deleted]"), post("old", 99)], "after": "p2"},
        ])
        result = self.collect(api)
        self.assertEqual([p["id"] for p in result["reports"]], ["a", "b"])
        self.assertEqual(result["status"], "window_boundary")
        self.assertEqual(api.calls[1][1]["after"], "p1")

    def test_partial_coverage_is_explicit(self):
        api = Pages([{"children": [post("a", 199)], "after": "p1"}])
        self.assertEqual(self.collect(api, 1)["status"], "page_limit")
        api = Pages([{"children": [], "after": "p1"}, {"children": [], "after": "p1"}])
        self.assertEqual(self.collect(api)["status"], "repeated_cursor")

    def test_empty_listing_and_timestamp_validation(self):
        self.assertEqual(self.collect(Pages([{"children": [], "after": None}]))["status"], "listing_exhausted")
        with self.assertRaises(ValueError):
            parse_utc("2026-10-15T12:00:00")
        self.assertEqual(parse_utc("2026-10-15T12:00:00+01:00").hour, 11)

    def test_later_page_failure_preserves_reviewable_reports(self):
        class FailingPages(Pages):
            def get(self, path, params):
                if self.calls:
                    raise AccessError("Reddit returned HTTP 429")
                return super().get(path, params)
        result = self.collect(FailingPages([{"children": [post("a", 150)], "after": "p1"}]))
        self.assertEqual(result["status"], "inaccessible")
        self.assertEqual(result["pages"], 1)
        self.assertEqual(result["reports"][0]["id"], "a")

    def test_malformed_listing_is_source_specific_and_redirects_refused(self):
        result = self.collect(Pages([{"children": [{}], "after": None}]))
        self.assertEqual(result["status"], "inaccessible")
        with self.assertRaises(AccessError):
            NoRedirects().redirect_request(None, None, 302, "", {}, "https://example.org")


if __name__ == "__main__":
    unittest.main()
