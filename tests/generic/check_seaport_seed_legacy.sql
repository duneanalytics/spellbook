-- this tests checks a nft trades model for every row in a seed file.
-- actual implementation in macros/test-helpers/check_seed.sql
{% test check_seaport_seed_legacy(model, blockchain=None, project=None, version=None) %}

    {%- set seed_file = ref('seaport_trades_seed') -%}
    {%- set seed_check_columns = ['buyer','seller'] -%}
    {%- set seed_matching_columns = ['block_date','blockchain','project','version','tx_hash','evt_index','nft_contract_address','token_id','sub_type','sub_idx'] -%}
    {%- set filter = {'blockchain':blockchain, 'project':project, 'version':version} -%}

    {{ check_seed_macro_legacy(model,seed_file,seed_matching_columns,seed_check_columns,filter) }}

{% endtest %}
