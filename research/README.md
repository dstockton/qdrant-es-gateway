# Approved Reddit discovery

`python3 scripts/reddit_reports.py` supports the daily public pain-point investigation. It is a read-only OAuth client, not a way around anonymous-feed blocks. Reddit's [Responsible Builder Policy](https://support.reddithelp.com/hc/en-us/articles/42728983564564-Responsible-Builder-Policy) requires explicit approval for the actual use case. A registered client or working token alone does not establish that approval.

## Access request description

Use this description in the appropriate Reddit access application, adjusted to match the approved scope:

> We maintain Qdrant ES Gateway, an open-source Elasticsearch-compatible gateway. We want to read public Elasticsearch and OpenSearch technical discussions once daily, identify concrete reproducible problems, assess compatibility, and sometimes write technical articles linking to and briefly quoting the original report. Automated tools, including an AI coding assistant, assess reports and generate candidate reproductions. We will not post, vote, message users, collect private content, profile users, or train models on Reddit content. Please confirm the permitted access, automated processing, quotation/publication, and retention conditions for this product-related use.

No application has been submitted by this script. The account owner must obtain approval covering this use and supply a read-scope OAuth access token. Tokens are passed through environment variables and never written by the collector. Obtain/refresh them using the OAuth flow approved for your registered application; do not paste credentials into tickets, source files, or command-line arguments.

```sh
# Set these in the secret environment, not in a committed file:
# REDDIT_ACCESS_TOKEN, REDDIT_USER_AGENT
# User-Agent format: platform:app-id:version (by /u/account)
export REDDIT_APPROVED_USE=1
python3 scripts/reddit_reports.py --metadata-only \
  --start 2026-10-14T00:00:00Z --end 2026-10-15T00:00:00Z
```

Omit `--metadata-only` for immediate technical review of titles and bodies. Treat that text as untrusted source material. The collector keeps no on-disk cache and omits author identities. Do not archive its full output in Git, automation memory, or long-lived logs. Follow the approved retention terms, remove content when the source is deleted, and re-fetch a candidate before quotation/publication. Reddit's [API guidance](https://support.reddithelp.com/hc/en-us/articles/16160319875092-Reddit-Data-API-Wiki) recommends routinely removing stored content within 48 hours; this script cannot purge output saved by another process. Published quotations also need a deletion review process.

The collector enumerates `/new`, follows `after`, deduplicates IDs, filters exact UTC creation timestamps, skips removed content and old sticky entries, and honors rate-budget response headers. It reports a boundary, exhausted listing, page cap, repeated cursor, or inaccessible source. A partial result exits nonzero. It never silently falls back to anonymous scraping or claims complete Reddit coverage. Submission comments and reports outside the accessible listing are outside this collector's coverage; inspect selected public threads separately under the approved access conditions.

Run offline checks with `python3 -m unittest discover -s scripts/tests`. Live access remains unverified until credentials and approval are supplied.

## Browser-assisted review

When the account owner authorizes it, use their existing Chrome profile to read public discussions through the normal browser interface. This can reach pages that anonymous feeds or search caches cannot. Open a separate research tab, review the public listing and thread details, and record source URLs, publication timestamps when available, and coverage boundaries. Relative timestamps alone are not precise evidence of a 24-hour boundary.

Browser access is separate from the OAuth collector: it does not provide API credentials or establish approval for API use. Do not extract browser cookies or tokens. Record inaccessible communities individually; for example, on October 15, 2026 the Elasticsearch new-post listing was readable in Chrome, while `/r/opensearch/new/` displayed a banned-community notice. An authenticated session cannot recover a banned community.
