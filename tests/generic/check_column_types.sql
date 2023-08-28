-- this tests checks the column types of a model
{% test check_column_types(model, column_types) %}
    {{ check_column_types_macro(model,column_types) }}
{% endtest %}

{% test check_columns_nft_base_trades(model) %}
    {%- set column_types = {
        'block_date':'date',
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
        'block_date':'date',
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
