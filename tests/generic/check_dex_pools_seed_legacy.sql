-- this tests checks that the tokenid and token address is accurate for hardcoded pool data
-- e.g curvefi pools & ellipsis pools
-- actual implementation in macros/test-helpers/check_seed.sql
{% test check_dex_pools_seed_legacy(model, blockchain=None, project=None, version=None) %}

    {%- set seed_file = ref('dex_pools_seed') -%}
    {%- set seed_check_columns = ['token_address'] -%}
    {%- set seed_matching_columns = ['pool','blockchain','project','version','token_type','token_id'] -%}
    {%- set filter = {'blockchain':blockchain, 'project':project, 'version':version} -%}

    {{ check_seed_macro_legacy(model,seed_file,seed_matching_columns,seed_check_columns,filter) }}

{% endtest %}
