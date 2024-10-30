{{ config(
    alias = 'mints',
    schema = 'nft',
    partition_by = ['blockchain','block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','evt_index','token_id','number_of_items'],
    post_hook='{{ expose_spells(\'["ethereum","bnb","optimism","arbitrum","zksync","arbitrum","avalanche_c","blast","celo","fantom","gnosis","linea","mantle","polygon","scroll","sei"]\',
                    "sector",
                    "nft",
                    \'["soispoke","umer_h_adil","hildobby","0xRob", "chuxin", "lgingerich", "hildobby"]\') }}')
}}


{% set native_mints = [
ref('nft_arbitrum_native_mints')
,ref('nft_avalanche_c_native_mints')
,ref('nft_base_native_mints')
,ref('nft_blast_native_mints')
,ref('nft_celo_native_mints')
,ref('nft_ethereum_native_mints')
,ref('nft_fantom_native_mints')
,ref('nft_gnosis_native_mints')
,ref('nft_linea_native_mints')
,ref('nft_mantle_native_mints')
,ref('nft_optimism_native_mints')
,ref('nft_polygon_native_mints')
,ref('nft_scroll_native_mints')
,ref('nft_sei_native_mints')
,ref('nft_zksync_native_mints')
,ref('nft_zora_native_mints')
] %}

WITH native_mints AS
(
    SELECT *
    FROM
    (
        {% for native_mint in native_mints %}
        SELECT
            blockchain,
            project,
            version,
            CAST(date_trunc('day', block_time) as date)  as block_date,
            CAST(date_trunc('month', block_time) as date)  as block_month,
            block_time,
            token_id,
            collection,
            amount_usd,
            token_standard,
            trade_type,
            number_of_items,
            trade_category,
            evt_type,
            seller,
            buyer,
            amount_original,
            amount_raw,
            currency_symbol,
            currency_contract,
            nft_contract_address,
            project_contract_address,
            aggregator_name,
            aggregator_address,
            tx_hash,
            block_number,
            tx_from,
            tx_to,
            evt_index
        FROM {{ native_mint }} as n
        {% if is_incremental() %}
        WHERE n.block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

SELECT *
FROM native_mints
