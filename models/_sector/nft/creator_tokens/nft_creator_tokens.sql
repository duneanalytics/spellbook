{{ config(

    alias = 'creator_tokens',
    schema = 'nft',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['chain','address'],
    post_hook='{{ expose_spells(\'["ethereum","bnb","optimism","arbitrum","base","polygon","avalanche_c"]\',
                    "sector",
                    "nft",
                    \'["limitbreak"]\') }}')
}}


{% set chain_creator_tokens = [
 ref('nft_ethereum_creator_tokens')
,ref('nft_arbitrum_creator_tokens')
,ref('nft_avalanche_c_creator_tokens')
,ref('nft_base_creator_tokens')
,ref('nft_bnb_creator_tokens')
,ref('nft_optimism_creator_tokens')
,ref('nft_polygon_creator_tokens')
] %}

WITH creator_tokens_union as
(
    {% for chain_creator_token in chain_creator_tokens %}
    SELECT
        blockchain, 
        is_creator_token, 
        token_type, 
        creation_time, 
        address, 
        is_clone,
        CAST(date_trunc('day', creation_time) as date)  as block_date,
        CAST(date_trunc('month', creation_time) as date)  as block_month
    FROM {{ chain_creator_token }}
    {% if is_incremental() %}
    AND creation_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

select * from creator_tokens_union