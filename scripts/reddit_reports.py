#!/usr/bin/env python3
"""Read an approved Reddit OAuth listing; emit exact-window evidence to stdout.

No posting endpoints, anonymous fallback, credential storage, or content cache.
"""
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import re
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class AccessError(Exception):
    pass


class Reddit:
    def __init__(self, token, user_agent, opener=urlopen, sleep=time.sleep):
        self.token, self.user_agent = token, user_agent
        self.opener, self.sleep = opener, sleep
        self.next_request = 0.0

    def get(self, path, params):
        self.sleep(max(0, self.next_request - time.monotonic()))
        request = Request("https://oauth.reddit.com" + path + "?" + urlencode(params),
                          headers={"Authorization": "Bearer " + self.token,
                                   "User-Agent": self.user_agent})
        try:
            with self.opener(request, timeout=30) as response:
                # Serialize requests and respect the service's remaining budget.
                delay = 1.0
                if float(response.headers.get("x-ratelimit-remaining", "1")) < 1:
                    delay = max(delay, float(response.headers.get("x-ratelimit-reset", "60")))
                self.next_request = time.monotonic() + delay
                return json.load(response)
        except HTTPError as error:
            # Do not print response bodies, request objects, or authorization headers.
            raise AccessError(f"Reddit returned HTTP {error.code}; no anonymous fallback. "
                              "Verify approval/token or retry after the service rate limit.") from None
        except (URLError, ValueError, TimeoutError):
            raise AccessError("Reddit request failed or returned invalid JSON/rate headers") from None


def collect(client, subreddit, start, end, max_pages=100):
    if not re.fullmatch(r"[A-Za-z0-9_]{2,32}", subreddit):
        raise ValueError("Invalid subreddit name")
    seen, cursors, reports = set(), set(), []
    after = None
    status = "page_limit"
    pages = 0
    for _ in range(max_pages):
        params = {"limit": 100, "raw_json": 1}
        if after:
            params["after"] = after
        listing = client.get(f"/r/{subreddit}/new", params)["data"]
        pages += 1
        children = listing["children"]
        reached_start = False
        for child in children:
            post = child["data"]
            created = float(post["created_utc"])
            # Sticky entries can be older than the newest-first listing itself.
            if created < start.timestamp() and not post.get("stickied", False):
                reached_start = True
            fullname = post["name"]
            if fullname in seen or not start.timestamp() <= created < end.timestamp():
                continue
            seen.add(fullname)
            if post.get("removed_by_category") or post.get("selftext") in ("[deleted]", "[removed]"):
                continue
            permalink = post["permalink"]
            if not permalink.startswith("/r/"):
                raise AccessError("Unexpected Reddit permalink")
            reports.append({"id": fullname, "url": "https://www.reddit.com" + permalink,
                            "created_utc": datetime.fromtimestamp(created, timezone.utc).isoformat(),
                            "title": post.get("title", ""), "body": post.get("selftext", "")})
        after = listing.get("after")
        if reached_start:
            status = "window_boundary"
            break
        if not after:
            status = "listing_exhausted"
            break
        if after in cursors:
            status = "repeated_cursor"
            break
        cursors.add(after)
    return {"subreddit": subreddit, "pages": pages, "status": status, "reports": reports}


def parse_utc(text):
    value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("Timestamp must include timezone")
    return value.astimezone(timezone.utc)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--end", help="Exclusive ISO8601 timestamp; default current UTC")
    parser.add_argument("--start", help="Inclusive timestamp; default 24 hours before end")
    parser.add_argument("--subreddit", action="append", default=[])
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--metadata-only", action="store_true", help="Omit titles and bodies")
    args = parser.parse_args()
    if os.getenv("REDDIT_APPROVED_USE") != "1":
        parser.error("Set REDDIT_APPROVED_USE=1 only after Reddit approves this use case")
    token, ua = os.getenv("REDDIT_ACCESS_TOKEN"), os.getenv("REDDIT_USER_AGENT")
    if not token or not ua or "(by /u/" not in ua:
        parser.error("REDDIT_ACCESS_TOKEN and descriptive REDDIT_USER_AGENT with contact are required")
    try:
        end = parse_utc(args.end) if args.end else datetime.now(timezone.utc)
        start = parse_utc(args.start) if args.start else end - timedelta(hours=24)
        if start >= end or args.max_pages < 1:
            raise ValueError("Start must precede end and max-pages must be positive")
        client = Reddit(token, ua)
        coverage = []
        for sub in dict.fromkeys(args.subreddit or ["elasticsearch", "opensearch"]):
            try:
                result = collect(client, sub, start, end, args.max_pages)
            except AccessError as error:
                result = {"subreddit": sub, "status": "inaccessible", "error": str(error), "reports": []}
            if args.metadata_only:
                for post in result["reports"]:
                    post.pop("title", None)
                    post.pop("body", None)
            coverage.append(result)
        print(json.dumps({"start": start.isoformat(), "end": end.isoformat(),
                          "coverage": coverage,
                          "scope": "New submissions in accessible listings, not comments or all public Reddit."}, indent=2))
        return int(any(x["status"] in ("inaccessible", "page_limit", "repeated_cursor") for x in coverage))
    except (ValueError, KeyError, TypeError) as error:
        print(f"Invalid input or listing: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
