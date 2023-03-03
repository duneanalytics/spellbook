{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                        "project",
                                        "pika",
                                        \'["msilb7", "drethereum", "rplust"]\') }}'
        )
}}

SELECT *
FROM
(
        SELECT
                blockchain
		,block_date
                ,block_time
                ,virtual_asset
                ,underlying_asset
                ,market
                ,market_address
                ,volume_usd
                ,fee_usd
                ,margin_usd
                ,trade
                ,project
                ,version
                ,trader
                ,volume_raw
                ,tx_hash
                ,tx_from
                ,tx_to
                ,evt_index
        FROM {{ ref('pika_v1_optimism_trades') }}
        UNION
        SELECT
                blockchain
		,block_date
                ,block_time
                ,virtual_asset
                ,underlying_asset
                ,market
                ,market_address
                ,volume_usd
                ,fee_usd
                ,margin_usd
                ,trade
                ,project
                ,version
                ,trader
                ,volume_raw
                ,tx_hash
                ,tx_from
                ,tx_to
                ,evt_index
        FROM {{ ref('pika_v2_optimism_trades') }}
        UNION
        SELECT
                blockchain
		,block_date
                ,block_time
                ,virtual_asset
                ,underlying_asset
                ,market
                ,market_address
                ,volume_usd
                ,fee_usd
                ,margin_usd
                ,trade
                ,project
                ,version
                ,trader
                ,volume_raw
                ,tx_hash
                ,tx_from
                ,tx_to
                ,evt_index
        FROM {{ ref('pika_v3_optimism_trades') }}
)