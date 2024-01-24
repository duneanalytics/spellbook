{{ config(

    alias = 'mints',
    schema = 'nft',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','evt_index','token_id','number_of_items'],
    post_hook='{{ expose_spells(\'["ethereum","bnb","optimism","arbitrum","zksync"]\',
                    "sector",
                    "nft",
                    \'["soispoke","umer_h_adil","hildobby","0xRob", "chuxin", "lgingerich"]\') }}')
}}


{% set native_mints = [
 ref('nft_ethereum_native_mints')
,ref('nft_optimism_native_mints')
,ref('nft_base_native_mints')
,ref('nft_zora_native_mints')
,ref('nft_zksync_native_mints')
] %}


{% set project_mints = [
 ref('opensea_v1_ethereum_events')
,ref('magiceden_solana_events')
] %}

WITH project_mints as
(
    {% for project_mint in project_mints %}
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
    FROM {{ project_mint }}
    WHERE evt_type = 'Mint'
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
, native_mints AS
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
        LEFT JOIN
            (
                select distinct
                    block_number as p_block_number
                    , tx_hash as p_tx_hash
                from project_mints
            ) p
            ON n.block_number = p.p_block_number
            AND n.tx_hash = p.p_tx_hash
        WHERE p.p_tx_hash is null
            {% if is_incremental() %}
            AND n.block_time >= date_trunc('day', now() - interval '7' Day)
            {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

SELECT *
FROM project_mints
UNION ALL
SELECT *
FROM native_mints
