{{ config(
        schema = 'cow_protocol',
        alias='trades',
        post_hook='{{ expose_spells(\'["ethereum", "gnosis", "arbitrum", "base", "avalanche_c"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith", "gentrexha", "olgafetisova"]\') }}'
        )
}}
SELECT *
FROM
(
        SELECT
            'ethereum' AS blockchain,
            'cow_protocol' AS project,
            '1' AS version,
            block_date,
            block_month,
            block_time,
            buy_token AS token_bought_symbol,
            sell_token AS token_sold_symbol,
            token_pair,
            units_bought AS token_bought_amount,
            units_sold AS token_sold_amount,
            atoms_bought AS token_bought_amount_raw,
            atoms_sold AS token_sold_amount_raw,
            usd_value AS amount_usd,
            buy_token_address AS token_bought_address,
            sell_token_address AS token_sold_address,
            trader AS taker,
            CAST(NULL AS VARBINARY) AS maker,
            project_contract_address,
            tx_hash,
            trader AS tx_from,
            receiver AS tx_to,
            trace_address,
            evt_index
        FROM {{ ref('cow_protocol_ethereum_trades') }}

        UNION ALL


        SELECT
            'gnosis' AS blockchain,
            'cow_protocol' AS project,
            '1' AS version,
            block_date,
            block_month,
            block_time,
            buy_token AS token_bought_symbol,
            sell_token AS token_sold_symbol,
            token_pair,
            units_bought AS token_bought_amount,
            units_sold AS token_sold_amount,
            atoms_bought AS token_bought_amount_raw,
            atoms_sold AS token_sold_amount_raw,
            usd_value AS amount_usd,
            buy_token_address AS token_bought_address,
            sell_token_address AS token_sold_address,
            trader AS taker,
            CAST(NULL AS VARBINARY) AS maker,
            project_contract_address,
            tx_hash,
            trader AS tx_from,
            receiver AS tx_to,
            trace_address,
            evt_index
        FROM {{ ref('cow_protocol_gnosis_trades') }}

        UNION ALL


        SELECT
            'arbitrum' AS blockchain,
            'cow_protocol' AS project,
            '1' AS version,
            block_date,
            block_month,
            block_time,
            buy_token AS token_bought_symbol,
            sell_token AS token_sold_symbol,
            token_pair,
            units_bought AS token_bought_amount,
            units_sold AS token_sold_amount,
            atoms_bought AS token_bought_amount_raw,
            atoms_sold AS token_sold_amount_raw,
            usd_value AS amount_usd,
            buy_token_address AS token_bought_address,
            sell_token_address AS token_sold_address,
            trader AS taker,
            CAST(NULL AS VARBINARY) AS maker,
            project_contract_address,
            tx_hash,
            trader AS tx_from,
            receiver AS tx_to,
            trace_address,
            evt_index
        FROM {{ ref('cow_protocol_arbitrum_trades') }}

        UNION ALL


        SELECT
            'base' AS blockchain,
            'cow_protocol' AS project,
            '1' AS version,
            block_date,
            block_month,
            block_time,
            buy_token AS token_bought_symbol,
            sell_token AS token_sold_symbol,
            token_pair,
            units_bought AS token_bought_amount,
            units_sold AS token_sold_amount,
            atoms_bought AS token_bought_amount_raw,
            atoms_sold AS token_sold_amount_raw,
            usd_value AS amount_usd,
            buy_token_address AS token_bought_address,
            sell_token_address AS token_sold_address,
            trader AS taker,
            CAST(NULL AS VARBINARY) AS maker,
            project_contract_address,
            tx_hash,
            trader AS tx_from,
            receiver AS tx_to,
            trace_address,
            evt_index
        FROM {{ ref('cow_protocol_base_trades') }}

        UNION ALL


        SELECT
            'avalanche_c' AS blockchain,
            'cow_protocol' AS project,
            '1' AS version,
            block_date,
            block_month,
            block_time,
            buy_token AS token_bought_symbol,
            sell_token AS token_sold_symbol,
            token_pair,
            units_bought AS token_bought_amount,
            units_sold AS token_sold_amount,
            atoms_bought AS token_bought_amount_raw,
            atoms_sold AS token_sold_amount_raw,
            usd_value AS amount_usd,
            buy_token_address AS token_bought_address,
            sell_token_address AS token_sold_address,
            trader AS taker,
            CAST(NULL AS VARBINARY) AS maker,
            project_contract_address,
            tx_hash,
            trader AS tx_from,
            receiver AS tx_to,
            trace_address,
            evt_index
        FROM {{ ref('cow_protocol_avalanche_c_trades') }}

        UNION ALL


        SELECT
            'polygon' AS blockchain,
            'cow_protocol' AS project,
            '1' AS version,
            block_date,
            block_month,
            block_time,
            buy_token AS token_bought_symbol,
            sell_token AS token_sold_symbol,
            token_pair,
            units_bought AS token_bought_amount,
            units_sold AS token_sold_amount,
            atoms_bought AS token_bought_amount_raw,
            atoms_sold AS token_sold_amount_raw,
            usd_value AS amount_usd,
            buy_token_address AS token_bought_address,
            sell_token_address AS token_sold_address,
            trader AS taker,
            CAST(NULL AS VARBINARY) AS maker,
            project_contract_address,
            tx_hash,
            trader AS tx_from,
            receiver AS tx_to,
            trace_address,
            evt_index
        FROM {{ ref('cow_protocol_polygon_trades') }}
)
