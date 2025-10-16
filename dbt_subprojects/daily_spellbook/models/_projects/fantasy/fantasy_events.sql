{{ config(
        schema = 'fantasy',
        alias = 'events',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["blast", "base"]\',
                        spell_type = "project",
                        spell_name = "events",
                        contributors = \'["hildobby"]\') }}',
        tags=['static']
        )
}}

SELECT block_time
, block_number
, block_date
, evt_type
, user_address
, whitelist
, collection
, cards_minted
, cards_burned
, minted_ids
, burned_ids
, traded_ids
, traded_with
, tx_from
, tx_to
, tx_hash
, tx_index
, contract_address
, is_wash_trade
, token_symbol
, token_address
, token_amount
, price_usd
, heroes_revenue
, heroes_revenue_usd
, to_fantasy_treasury
, to_fantasy_treasury_usd
, tactics_bought
FROM {{ ref('fantasy_blast_events')}}

UNION ALL

SELECT block_time
, block_number
, block_date
, evt_type
, user_address
, whitelist
, collection
, cards_minted
, cards_burned
, minted_ids
, burned_ids
, traded_ids
, traded_with
, tx_from
, tx_to
, tx_hash
, tx_index
, contract_address
, is_wash_trade
, token_symbol
, token_address
, token_amount
, price_usd
, heroes_revenue
, heroes_revenue_usd
, to_fantasy_treasury
, to_fantasy_treasury_usd
, tactics_bought
FROM {{ ref('fantasy_base_events')}}