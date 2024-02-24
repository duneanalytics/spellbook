{{ 
    config(
        schema = 'kreatorland_zksync',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% SET kreatorland_usage_start_date = "2023-06-25" %}

WITH base_trades AS (
    SELECT
        'zksync' AS blockchain,
        'kreatorland' AS project,
        '1.1' AS project_version,
        k.evt_block_time AS block_time,
        CAST(date_trunc('day', k.evt_block_time) AS DATE) AS block_date,
        CAST(date_trunc('month', k.evt_block_time) AS DATE) AS block_month,
        k.evt_block_number AS block_number,
        k. AS nft_contract_address,
        k. AS nft_token_id,
        k. AS nft_amount,
        k.offerer AS seller,
        k.recipient AS buyer,
        'Buy' AS trade_category,
        'secondary' AS trade_type,
        k. AS price_raw,
        0x000000000000000000000000000000000000800A AS currency_contract, -- ETH
        k.contract_address AS project_contract_address,
        k.evt_tx_hash AS tx_hash,
        CAST(NULL AS uint256) AS platform_fee_amount_raw,
        CAST(NULL AS uint256) AS royalty_fee_amount_raw,
        CAST(NULL AS varbinary) AS royalty_fee_address,
        CAST(NULL AS varbinary) AS platform_fee_address,
        k.evt_index AS sub_tx_trade_id
    FROM {{ source('kreator_land_zksync', 'Seaport_evt_OrderFulfilled') }} k
    {% IF is_incremental() %}
    WHERE {{incremental_predicate('k.evt_block_time')}}
    {% ELSE %}
    WHERE k.evt_block_time >= timestamp '{{kreatorland_usage_start_date}}'
    {% endif %}
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'zksync') }}
