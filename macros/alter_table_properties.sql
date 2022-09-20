{% macro alter_table_properties() %}
{%- if target.name == 'prod'-%}

{% set uniswap_v3_optimism_pools %}
ALTER VIEW uniswap_v3_optimism.pools SET TBLPROPERTIES ('dune.public'='true',
                                                'dune.data_explorer.blockchains'='["optimism"]',
                                                'dune.data_explorer.category'='abstraction',
                                                'dune.data_explorer.abstraction.type'='project',
                                                'dune.data_explorer.abstraction.name'='uniswap_v3',
                                                'dune.data_explorer.contributors'='["msilb7", "chuxinh"]');
{% endset %}

{% do run_query(prices_tokens) %}
{% do run_query(tornado_cash_deposits) %}
{% do run_query(tornado_cash_withdrawals) %}
{% do run_query(transfers_optimism_eth) %}
{% do run_query(uniswap_v3_optimism_pools) %}

{% do log("Tables generated", info=True) %}
{%- else -%}
{%- endif -%}
{% endmacro %}
