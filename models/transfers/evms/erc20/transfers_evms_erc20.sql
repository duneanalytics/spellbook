{{ config(materialized='view', alias = alias('erc20')
        , post_hook='{{ expose_spells(\'["evms"]\',
                                    "sector",
                                    "transfers",
                                    \'["longnhbkhn"]\') }}') }}
with evms_transfer as (
        select * from {{ ref('transfers_arbitrum_erc20') }}
        union all
        select * from {{ ref('transfers_avalanche_c_erc20') }}
        union all 
        select * from {{ ref('transfers_base_erc20') }}
        union all
        select * from {{ ref('transfers_celo_erc20') }}
        union all 
        select * from {{ ref('transfers_ethereum_erc20') }}
        union all
        select * from {{ ref('transfers_fantom_erc20') }}
        union all
        select * from {{ ref('transfers_bnb_bep20') }}
        union all
        select * from {{ ref('transfers_optimism_erc20') }}
        union all
        select * from {{ ref('transfers_polygon_erc20') }} 
    ), prices as (
        select *
        from {{ source('prices', 'usd') }}
        where blockchain is not null
    )

SELECT t.unique_transfer_id
    , t.blockchain
    , t.token_address
    , t.wallet_address
    , p.symbol
    , t.block_time
    , cast(t.amount_raw as uint256)
    , cast(t.amount_raw as double) / power(10, p.decimals) as amount
    , -1 * cast(t.amount_raw as double) / power(10, p.decimals) * p.price as amount_transfer_usd
FROM transfer_btc t
LEFT JOIN prices p
    ON date_trunc('minute', t.block_time) = p.minute
    and t.blockchain = p.blockchain
    and t.token_address = p.contract_address
    