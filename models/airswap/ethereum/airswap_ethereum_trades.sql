
{{ config(
    schema = 'airswap_ethereum',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(blockchains = \'["ethereum"]\',
                                spell_type = "project",
                                spell_name = "airswap",
                                contributors = \'["jeff-dude", "hosuke", "soispoke"]\') }}'
    )
}}

{% set project_start_date = '2019-12-20' %}

SELECT  blockchain,
        project,
        version,
        block_date,
        block_month,
        block_number,
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
        evt_index
FROM dex.trades
WHERE project = 'airswap'
  AND blockchain = 'ethereum'