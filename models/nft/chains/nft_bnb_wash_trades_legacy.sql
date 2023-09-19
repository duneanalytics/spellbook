{{ config(
	    tags=['legacy'],
        schema = 'nft_bnb',
        alias = alias('wash_trades', legacy_model=True),
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby"]\') }}',
        unique_key = ['unique_trade_id']
)
}}

SELECT NULL AS blockchain
, NULL AS project
, NULL AS version
, NULL AS nft_contract_address
, NULL AS token_id
, NULL AS token_standard
, NULL AS trade_category
, NULL AS buyer
, NULL AS seller
, NULL AS project_contract_address
, NULL AS aggregator_name
, NULL AS aggregator_address
, NULL AS tx_from
, NULL AS tx_to
, NULL AS block_time
, NULL AS block_date
, NULL AS block_number
, NULL AS tx_hash
, NULL AS unique_trade_id
, NULL AS buyer_first_funded_by
, NULL AS seller_first_funded_by
, NULL AS filter_1_same_buyer_seller
, NULL AS filter_2_back_and_forth_trade
, NULL AS filter_3_bought_or_sold_3x
, NULL AS filter_4_first_funded_by_same_wallet
, NULL AS filter_5_flashloan
, NULL AS is_wash_trade