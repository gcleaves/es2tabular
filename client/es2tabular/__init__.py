"""Python client for es2tabular.

Sends an Elasticsearch query to an es2tabular server and gets back a dataframe,
in one round trip. Nothing is stored on the server; run DuckDB locally against
what you get back.

    from es2tabular import Client

    es = Client()
    df = es.query("logs-*", {"size": 0, "aggs": {...}})

Configuration comes from the environment unless passed explicitly:

    ES2TABULAR_URL            e.g. https://misc.cleaves.ai/es2tabular
    ES2TABULAR_CLIENT_ID      Keycloak client-credentials client
    ES2TABULAR_CLIENT_SECRET
    ES2TABULAR_TOKEN_URL      Keycloak token endpoint
    ES2TABULAR_TOKEN          a bearer token, instead of the three above
"""

from __future__ import annotations

import os
import time
from io import BytesIO
from typing import Any

import httpx

__all__ = ["Client", "ES2TabularError", "AuthError"]

__version__ = "0.1.0"

# Cloudflare gives up on an origin after 100s and returns a 524. Failing just
# under that produces a clearer error than the edge's HTML error page.
DEFAULT_TIMEOUT = 90.0

# Refresh a little early rather than racing the expiry.
_TOKEN_REFRESH_MARGIN = 30.0


class ES2TabularError(RuntimeError):
    """A request to the es2tabular server failed."""


class AuthError(ES2TabularError):
    """Authentication or authorization failed."""


class Client:
    """A connection to an es2tabular server.

    Args:
        url: Base URL including any base path, e.g.
            ``https://misc.cleaves.ai/es2tabular``.
        client_id, client_secret, token_url: Keycloak client credentials. The
            client exchanges them for a short-lived bearer token and refreshes
            it as needed; there is no interactive login.
        token: A bearer token to use directly, instead of client credentials.
        timeout: Seconds to wait for a query.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        client_id: str | None = None,
        client_secret: str | None = None,
        token_url: str | None = None,
        token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        url = url or os.environ.get("ES2TABULAR_URL")
        if not url:
            raise ES2TabularError(
                "No server URL. Pass url=... or set ES2TABULAR_URL, e.g. "
                "https://misc.cleaves.ai/es2tabular"
            )
        self.url = url.rstrip("/")

        self._client_id = client_id or os.environ.get("ES2TABULAR_CLIENT_ID")
        self._client_secret = client_secret or os.environ.get("ES2TABULAR_CLIENT_SECRET")
        self._token_url = token_url or os.environ.get("ES2TABULAR_TOKEN_URL")
        self._static_token = token or os.environ.get("ES2TABULAR_TOKEN")

        if not self._static_token and not (
            self._client_id and self._client_secret and self._token_url
        ):
            raise ES2TabularError(
                "No credentials. Set ES2TABULAR_CLIENT_ID, ES2TABULAR_CLIENT_SECRET "
                "and ES2TABULAR_TOKEN_URL (or ES2TABULAR_TOKEN for a bearer token "
                "you already hold)."
            )

        self._http = httpx.Client(timeout=timeout, follow_redirects=False)
        self._token: str | None = None
        self._token_expires_at = 0.0

    # ---------------------------------------------------------------- auth

    def _access_token(self) -> str:
        """Return a valid bearer token, fetching or refreshing if needed."""
        if self._static_token:
            return self._static_token

        if self._token and time.monotonic() < self._token_expires_at:
            return self._token

        try:
            response = self._http.post(
                self._token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
        except httpx.RequestError as exc:
            raise AuthError(f"Could not reach the token endpoint: {exc}") from exc

        if response.status_code != 200:
            raise AuthError(
                f"Token request failed ({response.status_code}): {response.text[:400]}"
            )

        payload = response.json()
        self._token = payload["access_token"]
        self._token_expires_at = (
            time.monotonic() + float(payload.get("expires_in", 300)) - _TOKEN_REFRESH_MARGIN
        )
        return self._token

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._access_token()}"}

    # ------------------------------------------------------------ requests

    def _check(self, response: httpx.Response) -> None:
        """Turn a failed response into a useful exception."""
        if response.is_success:
            return

        content_type = response.headers.get("content-type", "")

        # oauth2-proxy answers an unauthenticated API call with a login page or
        # a redirect to one. Say so, rather than letting a JSON parse fail.
        if response.status_code in (301, 302, 303, 307, 308) or "text/html" in content_type:
            raise AuthError(
                f"Got a {response.status_code} and an HTML response rather than data. "
                "The bearer token was probably rejected: check the client's audience "
                "and preferred_username claims."
            )

        detail = response.text[:400]
        if "application/json" in content_type:
            try:
                body = response.json()
                detail = body.get("message") or body.get("error") or detail
            except ValueError:
                pass

        if response.status_code in (401, 403):
            raise AuthError(f"{response.status_code}: {detail}")
        raise ES2TabularError(f"{response.status_code}: {detail}")

    def whoami(self) -> dict[str, Any]:
        """Return the server's view of who you are. Useful for checking setup."""
        response = self._http.get(f"{self.url}/api/me", headers=self._headers())
        self._check(response)
        return response.json()

    # -------------------------------------------------------------- query

    def query(
        self,
        index: str,
        query: dict[str, Any],
        *,
        aggregation_name: str | None = None,
        to: str = "pandas",
        wire: str = "json",
    ):
        """Run an Elasticsearch query and return the tabular result.

        Args:
            index: Index pattern, e.g. ``"logs-*"``.
            query: The Elasticsearch query body, as you would write it in Kibana.
            aggregation_name: Which top-level aggregation to tabulate. Defaults
                to the first one in the response.
            to: ``"pandas"``, ``"polars"``, or ``"rows"`` for a list of dicts.
            wire: ``"json"`` (default) preserves types; ``"csv"`` transfers less
                for large results but leaves type inference to the reader.

        Returns:
            A dataframe, or a list of dicts when ``to="rows"``.
        """
        if to not in ("pandas", "polars", "rows"):
            raise ValueError(f"to must be 'pandas', 'polars' or 'rows', not {to!r}")
        if wire not in ("json", "csv"):
            raise ValueError(f"wire must be 'json' or 'csv', not {wire!r}")
        if wire == "csv" and to == "rows":
            raise ValueError("wire='csv' cannot produce rows; use wire='json'")

        body: dict[str, Any] = {"index": index, "query": query, "format": wire}
        if aggregation_name:
            body["aggregationName"] = aggregation_name

        try:
            response = self._http.post(
                f"{self.url}/api/query/table", json=body, headers=self._headers()
            )
        except httpx.TimeoutException as exc:
            raise ES2TabularError(
                f"The query did not finish within the client timeout: {exc}. "
                "Narrow the query, or raise timeout= if the server allows longer."
            ) from exc
        except httpx.RequestError as exc:
            raise ES2TabularError(f"Could not reach {self.url}: {exc}") from exc

        self._check(response)

        if wire == "csv":
            return _read_csv(response.content, to)

        payload = response.json()
        return _from_rows(payload.get("rows", []), payload.get("columns", []), to)

    # ------------------------------------------------------------ plumbing

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"Client(url={self.url!r})"


def _from_rows(rows: list[dict[str, Any]], columns: list[str], to: str):
    if to == "rows":
        return rows

    if to == "pandas":
        import pandas as pd

        # Passing columns keeps an empty result shaped like a real one.
        return pd.DataFrame(rows, columns=columns or None)

    import polars as pl

    if not rows:
        return pl.DataFrame({column: [] for column in columns})
    return pl.DataFrame(rows)


def _read_csv(content: bytes, to: str):
    if to == "pandas":
        import pandas as pd

        if not content.strip():
            return pd.DataFrame()
        return pd.read_csv(BytesIO(content))

    import polars as pl

    if not content.strip():
        return pl.DataFrame()
    return pl.read_csv(BytesIO(content))
