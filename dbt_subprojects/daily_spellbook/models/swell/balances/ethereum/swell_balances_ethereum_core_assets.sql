{{
  config(
        schema = 'swell_balances_ethereum',
        alias = 'core_assets',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'wallet_address', 'token_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "swell",
                                \'["maybeYonas"]\') }}'
  )
}}

with
tokens as (
  select * from (values
    (0xf951E335afb289353dc249e82926178EaC7DEd78,   'swETH', 'Swell LRT'),
    (0xFAe103DC9cf190eD75350761e95403b7b8aFa6c0,  'rswETH', 'Swell LRT'),
    (0x0a6E7Ba5042B38349e437ec6Db6214AEC7B35676,   'SWELL', 'Swell LRT'),
    (0x358d94b5b2F147D741088803d932Acb566acB7B6,  'rSWELL', 'Swell LRT'),
    (0x9Ed15383940CC380fAEF0a75edacE507cC775f22, 'earnETH', 'Swell LRT'),
    (0x66E47E6957B85Cf62564610B76dD206BB04d831a, 'earnBTC', 'Swell LRT'),
    (0x8DB2350D78aBc13f5673A411D4700BCF87864dDE,   'swBTC', 'Swell LRT')
  ) as t(
    token_address,
    symbol,
    name
  )
),
balances as (
    {{
      balances_incremental_subset_daily(
            blockchain = 'ethereum',
            token_list = 'tokens',
            start_date = '2023-04-12'
      )
    }}
)

select 
    -- t.name,
    b.blockchain,
    b.day,
    b.address as wallet_address,
    b.token_symbol,
    b.token_address,
    b.token_standard,
    b.token_id,
    b.balance,
    b.balance_usd,
    b.last_updated
from balances b
-- left join tokens t
--     on b.token_address = t.token_address
