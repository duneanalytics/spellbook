import json
import os
from pathlib import Path


def normalize_relation(relation_name, node):
    if relation_name:
        relation_parts = [
            part for part in relation_name.replace('"', "").replace("`", "").split(".") if part
        ]

        if len(relation_parts) >= 3:
            return f"dune.{relation_parts[-2]}.{relation_parts[-1]}"

        if len(relation_parts) == 2:
            return f"dune.{relation_parts[0]}.{relation_parts[1]}"

    if not node.get("schema"):
        return None

    return f"dune.{node['schema']}.{node.get('alias') or node['name']}"


def get_built_tables(manifest_path, run_results_path):
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {"nodes": {}}

    if not run_results_path.exists():
        return []

    built_tables = []
    seen_tables = set()
    run_results = json.loads(run_results_path.read_text())

    for result in run_results.get("results", []):
        node = manifest.get("nodes", {}).get(result.get("unique_id"))
        if not node or node.get("resource_type") != "model" or result.get("status") != "success":
            continue

        table_name = normalize_relation(result.get("relation_name"), node)
        if table_name and table_name not in seen_tables:
            seen_tables.add(table_name)
            built_tables.append(table_name)

    return built_tables


def build_comment(built_tables, project_name, run_url):
    visible_tables = built_tables[:5]
    table_lines = (
        "\n".join(f"- `{table_name}`" for table_name in visible_tables)
        if visible_tables
        else "No dbt model tables were reported as built by this workflow run."
    )
    truncation_note = (
        f"\n\nShowing 5 of {len(built_tables)} tables. See the [dbt run logs]({run_url}) for the rest."
        if len(built_tables) > 5
        else ""
    )
    query_note = (
        "\n\nYou can query these successfully built CI tables in the Dune app, MCP, or CLI using the fully qualified names above."
        if visible_tables
        else ""
    )
    run_note = "" if len(built_tables) > 5 else f"\n\n[View dbt run logs]({run_url})"
    marker = f"<!-- spellbook-dbt-run-built-tables:{project_name} -->"

    return "\n".join(
        [
            marker,
            f"### Queryable Dune CI Tables for `{project_name}`",
            "",
            "The dbt run succeeded and built these tables in the `dune` catalog:",
            "",
            f"{table_lines}{truncation_note}{query_note}{run_note}",
        ]
    )


def main():
    project_name = os.environ["PROJECT_NAME"]
    manifest_path = Path(os.environ["PROJECT_DIR"]) / "target" / "manifest.json"
    run_results_path = Path(os.environ["DBT_BUILT_RELATIONS_DIR"]) / "initial_run_results.json"
    built_tables = get_built_tables(manifest_path, run_results_path)
    body = build_comment(built_tables, project_name, os.environ["RUN_URL"])
    Path(os.environ["DBT_CI_TABLES_COMMENT_PATH"]).write_text(f"{body}\n")


if __name__ == "__main__":
    main()
