{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc721_noncompliant'),
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats", "0xBoxer", "tomfutago"]\') }}'
    )
}}

with 

multiple_owners as (
    select 
      blockchain,
      token_address,
      token_id,
      count(wallet_address) as holder_count --should always be 1
    from {{ ref('transfers_celo_erc721_rolling_day') }}
    where recency_index = 1
      and amount = 1
    group by 1,2,3
    having count(wallet_address) > 1
)

select distinct token_address from multiple_owners
