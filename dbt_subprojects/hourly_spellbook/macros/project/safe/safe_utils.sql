{% macro safe_native_transfer_wrapper(blockchain, native_token_symbol=none, project_start_date=none, date_filter=false) %}
{%- set network_config = get_safe_network_config(blockchain) -%}
{%- set token = native_token_symbol if native_token_symbol else network_config.native_token -%}
{%- set start_date = project_start_date if project_start_date else network_config.start_date -%}

{{ 
    safe_config(
        blockchain = blockchain,
        alias_name = token|lower ~ '_transfers',
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address']
    )
}}

{{
    safe_native_transfers(
        blockchain = blockchain,
        native_token_symbol = token,
        project_start_date = start_date,
        date_filter = date_filter
    )
}}
{% endmacro %}

{% macro safe_aggregation_all(table_type, blockchains=none, contributors=none) %}
{%- set all_networks = safe_network_config() -%}
{%- set networks_list = blockchains if blockchains else all_networks.keys()|list -%}

{%- set columns_by_type = {
    'transactions': [
        'blockchain',
        'block_date',
        'block_month',
        'block_time',
        'block_number',
        'tx_hash',
        'address',
        'to',
        'value',
        'gas',
        'execution_gas_used',
        'total_gas_used',
        'tx_index',
        'sub_traces',
        'trace_address',
        'success',
        'error',
        'code',
        'input',
        'output',
        'method'
    ],
    'safes': [
        'blockchain',
        'address',
        'creation_version',
        'block_date',
        'creation_time',
        'tx_hash'
    ],
    'native_transfers': [
        'blockchain',
        'symbol',
        'address',
        'block_date',
        'block_time',
        'amount_raw',
        'amount_usd',
        'tx_hash',
        'trace_address'
    ]
} -%}

{%- set columns = columns_by_type[table_type] -%}

{{ config(
    schema = 'gnosis_safe',
    alias = table_type ~ '_all',
    post_hook='{{ expose_spells(\'' ~ networks_list|tojson ~ '\',
                                "project",
                                "gnosis_safe",
                                \'' ~ (contributors if contributors else '["kryptaki", "danielpartida", "safeintern", "safehjc"]') ~ '\') }}'
    )
}}

{%- set model_refs = [] -%}
{%- for network in networks_list -%}
    {%- if table_type == 'native_transfers' -%}
        {%- set network_config = all_networks[network] -%}
        {%- if network_config.get('has_native_transfers', true) -%}
            {%- set _ = model_refs.append({'name': 'safe_' ~ network ~ '_' ~ network_config.native_token|lower ~ '_transfers'}) -%}
        {%- endif -%}
    {%- else -%}
        {%- set _ = model_refs.append({'name': 'safe_' ~ network ~ '_' ~ table_type}) -%}
    {%- endif -%}
{%- endfor -%}

{%- if model_refs|length > 0 %}
SELECT *
FROM (
    {%- for model in model_refs %}
    SELECT
        {%- for column in columns %}
        {{ column }}{{ "," if not loop.last }}
        {%- endfor %}
    FROM {{ ref(model.name) }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
    {%- endfor %}
)
{%- else %}
-- No models found for this aggregation
SELECT
    {%- for column in columns %}
    CAST(NULL AS {{ 'VARCHAR' if column in ['blockchain', 'symbol', 'tx_hash', 'address'] else 'TIMESTAMP' if column in ['block_time', 'creation_time'] else 'DATE' if column in ['block_date'] else 'VARBINARY' if column == 'trace_address' else 'DECIMAL(38,0)' }}) AS {{ column }}{{ "," if not loop.last }}
    {%- endfor %}
WHERE 1=0
{%- endif %}
{% endmacro %}

{% macro safe_transactions_wrapper(blockchain, project_start_date=none, date_filter=false) %}
{%- set network_config = get_safe_network_config(blockchain) -%}
{%- set start_date = project_start_date if project_start_date else network_config.start_date -%}

{{ 
    safe_config(
        blockchain = blockchain,
        alias_name = 'transactions'
    )
}}

{{ safe_transactions(blockchain, start_date, date_filter) }}
{% endmacro %}
