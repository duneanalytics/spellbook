{{
    config(
        schema = 'oneinch',
        alias = 'blockchains',
        materialized = 'view',
        unique_key = ['blockchain'],
    )
}}



{% for blockchain in oneinch_exposed_blockchains_list() %}
    {{ oneinch_blockchain_macro(blockchain) }}
    {% if not loop.last %}union all{% endif %}
{% endfor %}



-- -- FOR CI
-- select 
--     blockchain
--     , chain_id
--     , native_token_symbol
--     , wrapped_native_token_address
--     , explorer_link
--     , now() - interval '7' day as  first_deploy_at -- easy dates
-- from t