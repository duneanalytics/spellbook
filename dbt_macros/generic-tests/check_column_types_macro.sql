{% macro check_column_types_macro(model, column_types) %}
with test_sample as (
select * from {{model}} limit 1
)
, equality_checks as (
  {%- for col, col_type in column_types.items() %}
  select '{{col}}' column_name, '{{col_type}}' as expected_type, typeof({{col}}) as actual_type
  from test_sample
  {% if not loop.last %}union all{% endif %}
  {% endfor -%}
)
select * from equality_checks where not contains(split(expected_type,'/'),actual_type)
{% endmacro %}

-- this tests checks the column types of a model
{% test check_column_types(model, column_types) %}
    {{ check_column_types_macro(model,column_types) }}
{% endtest %}

{% test check_columns_nft_base_trades(model) %}
    {%- set column_types = {
        'block_time':'timestamp(3) with time zone',
        'block_number':'bigint',
        'tx_hash':'varbinary',
        'project_contract_address':'varbinary',
        'trade_category':'varchar',
        'trade_type':'varchar',
        'buyer':'varbinary',
        'seller':'varbinary',
        'nft_contract_address':'varbinary',
        'nft_token_id':'uint256',
        'nft_amount':'uint256',
        'currency_contract':'varbinary',
        'price_raw':'uint256',
        'platform_fee_amount_raw':'uint256',
        'royalty_fee_amount_raw':'uint256',
        'platform_fee_address':'varbinary',
        'royalty_fee_address':'varbinary',
        'sub_tx_trade_id':'integer/bigint'
    } -%}
   {{ check_column_types_macro(model,column_types) }}
{% endtest %}

{% test check_columns_nft_old_events(model) %}
    {%- set column_types = {
        'block_time':'timestamp(3) with time zone',
        'block_number':'bigint',
        'tx_hash':'varbinary',
        'project_contract_address':'varbinary',
        'trade_category':'varchar',
        'trade_type':'varchar',
        'buyer':'varbinary',
        'seller':'varbinary',
        'nft_contract_address':'varbinary',
        'token_id':'uint256',
        'number_of_items':'uint256',
        'currency_contract':'varbinary',
        'amount_raw':'uint256',
        'platform_fee_amount_raw':'uint256',
        'royalty_fee_amount_raw':'uint256',
        'royalty_fee_receive_address':'varbinary',
        'unique_trade_id':'varchar'
    } -%}
   {{ check_column_types_macro(model,column_types) }}
{% endtest %}

{% test check_columns_solana_dex_trades(model) %}
    {%- set column_types = {
        'blockchain': 'varchar',
        'project': 'varchar',
        'version': 'integer',
        'block_month': 'date',
        'block_slot': 'bigint',
        'block_time': 'timestamp(3) with time zone',
        'trade_source': 'varchar',
        'token_bought_amount_raw': 'uint256',
        'token_sold_amount_raw': 'uint256',
        'fee_tier': 'double',
        'token_bought_mint_address': 'varchar',
        'token_sold_mint_address': 'varchar',
        'token_bought_vault': 'varchar',
        'token_sold_vault': 'varchar',
        'project_program_id': 'varchar',
        'trader_id': 'varchar',
        'tx_id': 'varchar',
        'outer_instruction_index': 'integer',
        'inner_instruction_index': 'integer'
    } -%}
   {{ check_column_types_macro(model,column_types) }}
{% endtest %}
