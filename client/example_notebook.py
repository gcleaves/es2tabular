# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "es2tabular @ git+https://github.com/gcleaves/es2tabular.git#subdirectory=client",
#     "pandas",
#     "duckdb",
# ]
# ///
"""Example marimo notebook: query Elasticsearch through es2tabular, analyse locally.

The server returns rows; DuckDB runs here in the kernel, against the dataframe.
"""

import marimo

app = marimo.App(width="medium")


@app.cell
def _():
    import json

    import marimo as mo

    from es2tabular import Client

    # Credentials come from the project's Environment & cloud access:
    # ES2TABULAR_URL, ES2TABULAR_TOKEN_URL, ES2TABULAR_CLIENT_ID, ES2TABULAR_CLIENT_SECRET
    es = Client()
    return es, json, mo


@app.cell
def _(es, mo):
    mo.md(f"Connected to `{es.url}` as `{es.whoami()}`")
    return


@app.cell
def _(mo):
    DEFAULT_QUERY = """{
  "size": 0,
  "aggs": {
    "by_client": {
      "terms": { "field": "client_id", "size": 20 },
      "aggs": {
        "by_status": { "terms": { "field": "status" } }
      }
    }
  }
}"""

    index = mo.ui.text(value="logs-*", full_width=True)
    query = mo.ui.code_editor(value=DEFAULT_QUERY, language="json")
    run = mo.ui.run_button(label="Run query")

    mo.vstack([mo.md("**Index pattern**"), index, mo.md("**Query**"), query, run])
    return index, query, run


@app.cell
def _(es, index, json, mo, query, run):
    # Nothing runs until the button is pressed, so editing the query is free.
    mo.stop(not run.value, mo.md("Press **Run query**."))

    df = es.query(index.value, json.loads(query.value))
    df
    return (df,)


@app.cell
def _(df, mo):
    # DuckDB in this kernel, querying the dataframe directly. No server round trip.
    top = mo.sql(
        """
        SELECT by_client AS client, sum(doc_count) AS docs
        FROM df
        GROUP BY client
        ORDER BY docs DESC
        LIMIT 10
        """
    )
    return (top,)


if __name__ == "__main__":
    app.run()
