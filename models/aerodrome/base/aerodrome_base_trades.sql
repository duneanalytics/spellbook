{{ config(
    schema = 'aerodrome_base',
    alias = 'trades',
    materialized = 'view',
    post_hook='{{ expose_spells(blockchains = \'["base"]\',
                                spell_type = "project",
                                spell_name = "aerodrome",
                                contributors = \'["msilb7"]\') }}'
    )
}}

{% set project_start_date = '2023-08-01' %}

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
FROM ref('dex_trades')
WHERE project = 'aerodrome'
  AND blockchain = 'base'