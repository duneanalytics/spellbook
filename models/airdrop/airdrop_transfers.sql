{{ config(
        schema = 'airdrop',
        alias ='transfers',
        post_hook='{{ expose_spells(\'["optimism","ethereum"]\',
                                "sector",
                                "airdrop",
                                \'["msilb7"]\') }}'
        )
}}


{% set drop_models = [
  ref('optimism_op_airdrop_transfers')
 ,ref('velodrome_airdrop_transfers')
 ,ref('ens_airdrop_transfers')
 ,ref('looksrare_airdrop_transfers')
 ,ref('hop_protocol_airdrop_transfers')
 ,ref('uniswap_airdrop_transfers')
 ,ref('gitcoin_airdrop_transfers')
 ,ref('apecoin_airdrop_transfers')
] %}


SELECT *
FROM (
    {% for drop_mod in drop_models %}
    SELECT
          lower(blockchain) AS blockchain
        , airdrop_name
        , airdrop_project
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
