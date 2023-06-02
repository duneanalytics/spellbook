{{ config(
    tags=['prod_exclude'],
    alias ='mints',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_trade_id', 'blockchain'],
    post_hook='{{ expose_spells(\'["ethereum","solana","bnb","optimism","arbitrum","polygon"]\',
                    "sector",
                    "nft",
                    \'["soispoke","umer_h_adil","hildobby","0xRob", "chuxin"]\') }}')
}}


{% set native_mints = [
 ref('nft_ethereum_native_mints')
,ref('nft_optimism_native_mints')
] %}

WITH project_mints as
(
    SELECT
            blockchain,
            project,
            version,
            date_trunc('day', block_time)  as block_date,
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
            unique_trade_id
        FROM {{ ref('nft_events') }}
        WHERE evt_type = "Mint"
        {% if is_incremental() %}
        AND block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
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
            date_trunc('day', block_time)  as block_date,
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
            unique_trade_id
        FROM {{ native_mint }} as n
        LEFT JOIN
            (
                select
                    block_number as p_block_number
                    , tx_hash as p_tx_hash
                from project_mints
            ) p
            ON n.block_number = p.p_block_number
            AND n.tx_hash = p.p_tx_hash
        WHERE p.p_tx_hash is null
            {% if is_incremental() %}
            AND n.block_time >= date_trunc("day", now() - interval '1 week')
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
