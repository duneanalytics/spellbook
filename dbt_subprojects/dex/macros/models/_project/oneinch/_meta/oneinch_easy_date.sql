-- TEMP, CI ONLY -- REVERT THIS COMMIT BEFORE MERGE --
-- Clamps every oneinch model's start date to a rolling two-day window so CI builds only
-- recent data while shared macro changes force the complete oneinch lineage to rebuild.
{% macro oneinch_easy_date() %}
    {{ return((modules.datetime.date.today() - modules.datetime.timedelta(days=2)).isoformat()) }}
{% endmacro %}
