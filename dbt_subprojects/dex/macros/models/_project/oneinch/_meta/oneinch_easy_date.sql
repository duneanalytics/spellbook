-- TEMP, CI ONLY -- REVERT THIS COMMIT BEFORE MERGE --
-- Clamps every oneinch model's start date to a recent date so CI builds scan only a few days
-- of data instead of full history (CI rebuilds all oneinch models because shared macros changed).
-- Production tables must be built with full history: revert before merge + full refresh as needed.
{% macro oneinch_easy_date() %}
    {{ return("2026-07-20") }}
{% endmacro %}
