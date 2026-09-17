# es2tabular Python client

Send an Elasticsearch query to an [es2tabular](https://github.com/gcleaves/es2tabular)
server and get a dataframe back, in one round trip. The server converts the
aggregation response to rows and returns them; nothing is stored server-side.

## Install

```bash
pip install "es2tabular @ git+https://github.com/gcleaves/es2tabular.git#subdirectory=client"
```

In a marimo notebook, declare it in the inline script metadata so the kernel
provisions it:

```python
# /// script
# dependencies = [
#   "es2tabular @ git+https://github.com/gcleaves/es2tabular.git#subdirectory=client",
#   "pandas",
#   "duckdb",
# ]
# ///
```

## Configure

The client reads these environment variables. In marimohub, set them in the
project's **Environment & cloud access**, not a workspace `.env` — dotfiles are
dropped when a workspace is captured.

| Variable | Example |
| --- | --- |
| `ES2TABULAR_URL` | `https://misc.cleaves.ai/es2tabular` |
| `ES2TABULAR_TOKEN_URL` | `https://auth.example.com/realms/REALM/protocol/openid-connect/token` |
| `ES2TABULAR_CLIENT_ID` | `es2tabular-notebook` |
| `ES2TABULAR_CLIENT_SECRET` | *(secret field)* |

There is no interactive login. The client posts its client ID and secret to
Keycloak's token endpoint, gets a short-lived JWT back, caches it until it
expires, and sends it as `Authorization: Bearer`. oauth2-proxy accepts that in
place of a browser session.

Set `ES2TABULAR_TOKEN` instead to use a bearer token you already hold.

## Use

```python
from es2tabular import Client

es = Client()

df = es.query("logs-*", {
    "size": 0,
    "aggs": {
        "by_client": {
            "terms": {"field": "client_id", "size": 20},
            "aggs": {"by_status": {"terms": {"field": "status"}}},
        }
    },
})
```

`query()` takes:

- `index` — index pattern, e.g. `"logs-*"`
- `query` — the query body, exactly as you would write it in Kibana
- `aggregation_name` — which top-level aggregation to tabulate (default: the first)
- `to` — `"pandas"` (default), `"polars"`, or `"rows"` for a list of dicts
- `wire` — `"json"` (default, preserves types) or `"csv"` (less to transfer on
  large results, at the cost of type inference)

`es.whoami()` returns the server's view of your identity, which is the quickest
way to check that credentials work.

## Notes

- Non-aggregated queries return `hits.hits`, so they are capped by the query's
  `size`. There is no scroll or `search_after` support.
- The default timeout is 90 seconds, just under Cloudflare's 100-second origin
  timeout, so a slow query fails with a clear error rather than an HTML 524.
