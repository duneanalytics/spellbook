{% macro is_incremental() %}
    {#-- allow for command line compile of incremental models  #}
    {#-- Usage: dbt compile --vars '{force-incremental: True}'  #}
    {% if var('force-incremental', False) %}
        {{ return(True) }}
    {% endif %}

    {#-- do not run introspective queries in parsing #}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
        {{ return(relation is not none
                  and relation.type == 'table'
                  and model.config.materialized == 'incremental'
                  and not should_full_refresh()) }}
    {% endif %}
{% endmacro %}
