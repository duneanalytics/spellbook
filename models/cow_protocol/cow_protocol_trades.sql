{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith", "gentrexha"]\') }}'
        )
}}
SELECT *
FROM
(
        SELECT
            'ethereum' AS blockchain,
            'CoW Protocol' AS project,
            '1' AS version,
            block_date,
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
            NULL AS maker,
            project_contract_address,
            tx_hash,
            trader AS tx_from,
            receiver AS tx_to,
            '' AS trace_address,
            evt_index,
            'cow_protocol' ||'-'|| tx_hash ||'-'|| order_uid || evt_index AS unique_trade_id
        FROM {{ ref('cow_protocol_ethereum_trades') }}
        /*
        UNION
        <add future chains here>
        */
)