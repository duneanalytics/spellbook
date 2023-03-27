-- this tests checks a nft trades model for every row in a seed file.
-- actual implementation in macros/test-helpers/check_seed.sql
{% test check_opensea_seed(model, blockchain=None, project=None, version=None) %}

    {%- set seed_file = ref('opensea_trades_seed') -%}
    {%- set seed_check_columns = ['buyer','seller','amount_raw','platform_fee_amount_raw','royalty_fee_amount_raw'] -%}
    {%- set seed_matching_columns = ['block_date','blockchain','project','version','tx_hash','evt_index','sub_type','sub_idx','nft_contract_address','token_id'] -%}
    {%- set filter = {'blockchain':blockchain, 'project':project, 'version':version} -%}

    {{ check_seed_macro(model,seed_file,seed_matching_columns,seed_check_columns,filter) }}

{% endtest %}
