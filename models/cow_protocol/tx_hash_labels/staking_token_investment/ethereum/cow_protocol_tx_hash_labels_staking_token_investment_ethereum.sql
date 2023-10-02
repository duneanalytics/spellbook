{{
    config(
        alias = alias('tx_hash_labels_staking_token_investment_ethereum'),
        tags=['dunesql']
    )
}}

with
  staking_tokens as (
    select
        staking_token_address
    from
        (
            -- Taken from: https://www.coingecko.com/en/categories/liquid-staking-tokens
            VALUES  (0xae7ab96520de3a18e5e111b5eaab095312d7fe84), -- STETH
                    (0xae78736cd615f374d3085123a210448e74fc6393), -- RETH
                    (0x5e8422345238f34275888049021821e8e08caa1f), -- FRXETH
                    (0xe95a203b1a91a908f9b9ce46459d101078c2c3cb), -- ANKRETH
                    (0xac3e018457b222d93114458476f3e3416abbe38f), -- SFRXETH
                    (0xf03a7eb46d01d9ecaa104558c732cf82f6b6b645), -- MATICX
                    (0x20bc832ca081b91433ff6c17f85701b6e92486c5), -- RETH2
                    (0x628ebc64a38269e031afbdd3c5ba857483b5d048), -- LSETH
                    (0xc3d088842dcf02c13699f936bb83dfbbc6f721ab), -- VETH
                    (0x44017598f2af1bd733f9d87b5017b4e7c1b28dde), -- STKATOM
                    (0x45e007750cc74b1d2b4dd7072230278d9602c499), -- STKXPRT
                    (0x9ee91f9f426fa633d227f7a9b000e28b9dfd8599) -- STMATIC
        ) t(staking_token_address)
 ),

 staking_token_investment_trades as (
    select
        *
    from (
        select tx_hash, evt_index, project, version
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select staking_token_address from staking_tokens)
        UNION ALL
        select tx_hash, evt_index, project, version
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select staking_token_address from staking_tokens)
    )
 )

select
  'ethereum' as blockchain,
  concat(CAST(tx_hash AS VARCHAR), CAST(evt_index AS VARCHAR), project, version) as tx_hash_key,
  'Staking token investment' AS name,
  'tx_hash' AS category,
  'gentrexha' AS contributor,
  'query' AS source,
  TIMESTAMP '2023-02-23' as created_at,
  now() as updated_at,
  'staking_token_investment' as model_name,
  'usage' as label_type
from
  staking_token_investment_trades
