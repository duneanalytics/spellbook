{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_q1_2020',
    materialized = 'table',
    file_format = 'delta',
    tags = ['quarterly']
) }}

{{ solana_vote_fees_template("'2020-01-01'", "'2020-04-01'") }}
