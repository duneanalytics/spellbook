{{ config(
    alias ='trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "kyber",
                                    '["TODO: migrator name"]') }}'
    )
}}


TODO: Add logic for trades incremental table. Final query must include the following columns.
      See sample of how incremental tables work in models/airswap/ethereum/airswap_ethereum_trades.sql
      Pay attention to how {% if is_incremental() %} and  {% if not is_incremental() %} blocks are used.


            SELECT
                blockchain,
                project,
                version,
                block_date,
                block_time,
                token_bought_symbol,
                token_sold_symbol,
                token_pair,
                token_bought_amount,
                token_sold_amount,
                token_bought_amount_raw,
                token_sold_amount_raw,
                amount_usd,
                token_bought_address,
                token_sold_address,
                taker,
                maker,
                project_contract_address,
                tx_hash,
                tx_from,
                tx_to,
                trace_address,
                evt_index
        from
