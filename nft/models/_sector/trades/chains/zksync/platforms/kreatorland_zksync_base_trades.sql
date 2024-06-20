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

{% set kreatorland_usage_start_date = "2023-06-25" %}

WITH 
    base AS (
        SELECT 
            -- Consideration 1
            json_extract_scalar(consideration[1], '$.itemType') AS cons_item_type_1
            , from_hex(json_extract_scalar(consideration[1], '$.token')) AS cons_token_1
            , CAST(json_extract_scalar(consideration[1], '$.identifier') AS uint256) AS cons_identifier_1
            , CAST(json_extract_scalar(consideration[1], '$.amount') AS uint256) AS cons_amount_1
            , from_hex(json_extract_scalar(consideration[1], '$.recipient')) AS cons_recipient_1
            -- Consideration 2
            , json_extract_scalar(consideration[2], '$.itemType') AS cons_item_type_2
            , from_hex(json_extract_scalar(consideration[2], '$.token')) AS cons_token_2
            , CAST(json_extract_scalar(consideration[2], '$.identifier') AS uint256) AS cons_identifier_2
            , CAST(json_extract_scalar(consideration[2], '$.amount') AS uint256) AS cons_amount_2
            , from_hex(json_extract_scalar(consideration[2], '$.recipient')) AS cons_recipient_2
            -- Consideration 3
            , IF(cardinality(consideration)=3, json_extract_scalar(consideration[3], '$.itemType'), NULL) AS cons_item_type_3
            , IF(cardinality(consideration)=3, from_hex(json_extract_scalar(consideration[3], '$.token')), NULL) AS cons_token_3
            , IF(cardinality(consideration)=3, CAST(json_extract_scalar(consideration[3], '$.identifier') AS uint256), NULL) AS cons_identifier_3
            , IF(cardinality(consideration)=3, CAST(json_extract_scalar(consideration[3], '$.amount') AS uint256), CAST(0 AS uint256)) AS cons_amount_3
            , IF(cardinality(consideration)=3, from_hex(json_extract_scalar(consideration[3], '$.recipient')), NULL) AS cons_recipient_3
            -- Offer
            , json_extract_scalar(offer[1], '$.itemType') AS offer_item_type
            , from_hex(json_extract_scalar(offer[1], '$.token')) AS offer_token
            , CAST(json_extract_scalar(offer[1], '$.identifier') AS uint256) AS offer_identifier
            , CAST(json_extract_scalar(offer[1], '$.amount') AS uint256) AS offer_amount
            , from_hex(json_extract_scalar(offer[1], '$.recipient')) AS offer_recipient
            , *
        FROM {{ source('kreator_land_zksync', 'Seaport_evt_OrderFulfilled') }} 
        {% if is_incremental() %}
        WHERE {{incremental_predicate('evt_block_time')}}
        {% else %}
        WHERE evt_block_time >= timestamp '{{kreatorland_usage_start_date}}'
        {% endif %}
    )

    , base_trades AS (
        SELECT
            'zksync' AS blockchain
            , 'kreatorland' AS project 
            , 'v1.1' AS project_version
            , evt_tx_hash AS tx_hash
            , evt_index AS sub_tx_trade_id
            , CAST(date_trunc('day', evt_block_time) AS DATE) AS block_date
            , CAST(date_trunc('month', evt_block_time) AS DATE) AS block_month
            , evt_block_time AS block_time
            , evt_block_number AS block_number
            , contract_address AS project_contract_address
            , offerer AS seller
            , recipient AS buyer
            , 'secondary' AS trade_type
            , 'Buy' AS trade_category
            , cons_amount_1 + cons_amount_2 + cons_amount_3 AS price_raw
            , 0x000000000000000000000000000000000000800A AS currency_contract
            , cons_amount_3 AS royalty_fee_amount_raw
            , cons_recipient_3 AS royalty_fee_address
            , cons_amount_1 AS  platform_fee_amount_raw
            , cons_recipient_1 AS platform_fee_address
            , offer_token AS nft_contract_address
            , offer_identifier AS nft_token_id
            , offer_amount AS nft_amount
        FROM base 
        WHERE offer_item_type = '2' 
    )

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'zksync') }}
