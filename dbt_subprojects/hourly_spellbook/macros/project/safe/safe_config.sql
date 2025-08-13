{% macro safe_config(blockchain, alias_name, partition_by=['block_month'], unique_key=none, contributors=none, on_schema_change='fail', file_format='delta', incremental_strategy='merge', schema=none) %}
    {%- if not contributors -%}
        {%- set contributor_list = get_safe_contributors(blockchain, alias_name) -%}
        {%- set contributors = '\'' ~ contributor_list|tojson ~ '\'' -%}
    {%- endif -%}
    {{
        config(
            materialized='incremental',
            schema = schema if schema else 'gnosis_safe_' ~ blockchain,
            alias = alias_name,
            partition_by = partition_by,
            unique_key = unique_key if unique_key else default_safe_unique_key(alias_name),
            on_schema_change = on_schema_change,
            file_format = file_format,
            incremental_strategy = incremental_strategy,
            post_hook='{{ expose_spells(blockchains = \'["' ~ blockchain ~ '"]\',
                                        spell_type = "project",
                                        spell_name = "gnosis_safe",
                                        contributors = ' ~ contributors ~ ') }}'
        )
    }}
{% endmacro %}

{% macro safe_table_config(blockchain, alias_name, schema_prefix='safe') %}
    {#-
    Simplified config for Safe table models (non-incremental)
    Used primarily for singleton models
    #}
    {%- set contributors = get_safe_contributors(blockchain, alias_name) -%}
    {%- set contributors_str = '\'' ~ contributors|tojson ~ '\'' -%}
    {{
        config(
            materialized='table',
            schema = schema_prefix ~ '_' ~ blockchain,
            alias = alias_name,
            post_hook='{{ expose_spells(\'["' ~ blockchain ~ '"]\',
                                        "project",
                                        "safe",
                                        ' ~ contributors_str ~ ') }}'
        )
    }}
{% endmacro %}

{% macro default_safe_unique_key(alias_name) %}
    {%- if alias_name == 'safes' -%}
        ['block_date', 'address']
    {%- elif alias_name == 'transactions' -%}
        ['block_date', 'tx_hash', 'trace_address']
    {%- elif alias_name in ['eth_transfers', 'matic_transfers', 'xdai_transfers', 'bnb_transfers', 'avax_transfers', 'mnt_transfers', 'celo_transfers'] -%}
        ['block_date', 'address', 'tx_hash', 'trace_address']
    {%- elif alias_name == 'singletons' -%}
        ['address']
    {%- else -%}
        ['block_date']
    {%- endif -%}
{% endmacro %}
