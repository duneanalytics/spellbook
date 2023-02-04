{{ config(
        schema = 'airdrop_optimism'
        alias ='transfers',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "airdrop",
                                \'["msilb7"]\') }}'
        )
}}


{% set drop_models = [
 ref('airdrop_optimism_transfers_1')
] %}


SELECT *
FROM (
    {% for drop_mod in drop_models %}
    SELECT
          'optimism' AS blockchain
        , airdrop_name
        , distributor_address
        , recipient_address
        , airdrop_token_address
        , airdrop_token_symbol
        , transfer_block_date
        , transfer_block_time
        , transfer_block_number
        , transfer_tx_hash
        , transfer_evt_index
        , airdrop_token_amount
        , airdrop_token_amount_raw
        , tx_from_address
        , tx_to_address
        , tx_method_id
    FROM {{ drop_mod }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
