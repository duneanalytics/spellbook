-- TEMP, CI ONLY -- REVERT THIS COMMIT BEFORE MERGE --
-- Clamps every oneinch model's start date to a rolling ~2-day window so CI builds scan only
-- a couple of days of data instead of full history (CI rebuilds all oneinch models because
-- shared macros changed). Resolved at compile time; upstream models always compile before
-- downstream ones, so the window can only widen down the lineage, never gap.
-- Production tables must be built with full history: revert before merge + full refresh as needed.
{% macro oneinch_easy_date() %}
    {{ return((modules.datetime.date.today() - modules.datetime.timedelta(days=2)).isoformat()) }}
{% endmacro %}
