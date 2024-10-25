{{ config(
    schema = 'gas_solana',
    alias = 'tx_fees_q1_2020',
    materialized = 'table',
    file_format = 'delta',
    tags = ['quarterly']
) }}

{{ solana_tx_fees_template("'2020-01-01'", "'2020-04-01'") }}
