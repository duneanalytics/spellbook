{{ config(

    alias = 'creator_tokens',
    schema = 'limitbreak',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain','address'],
    post_hook='{{ expose_spells(\'["ethereum","bnb","optimism","arbitrum","base","polygon","avalanche_c"]\',
                    "sector",
                    "limitbreak",
                    \'["limitbreak"]\') }}')
}}


{% set chain_creator_tokens = [
 ref('limitbreak_ethereum_creator_tokens')
,ref('limitbreak_arbitrum_creator_tokens')
,ref('limitbreak_avalanche_c_creator_tokens')
,ref('limitbreak_base_creator_tokens')
,ref('limitbreak_bnb_creator_tokens')
,ref('limitbreak_optimism_creator_tokens')
,ref('limitbreak_polygon_creator_tokens')
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
        block_date,
        block_month
    FROM {{ chain_creator_token }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('creation_time')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

select * from creator_tokens_union