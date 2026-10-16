-- TEMP, CI ONLY -- REVERT THIS COMMIT BEFORE MERGE --
-- Clamps every oneinch model's start date to a recent date so CI builds scan only a few days
-- of data instead of full history (CI rebuilds all oneinch models because shared macros changed).
-- Production tables must be built with full history: revert before merge + full refresh as needed.
-- The rolling window keeps the CI scan bounded as the PR waits for review.
{% macro oneinch_easy_date() %}
    {{ return((modules.datetime.date.today() - modules.datetime.timedelta(days=7)).isoformat()) }}
{% endmacro %}
