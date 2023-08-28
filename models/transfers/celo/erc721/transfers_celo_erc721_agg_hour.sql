{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc721_agg_hour'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['wallet_address', 'token_address', 'block_hour'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago"]\') }}'
    )
}}

select
  'celo' as blockchain,
  cast(date_trunc('month', tr.block_time) as date) as block_month,
  date_trunc('hour', tr.block_time) as block_hour,
  tr.wallet_address,
  tr.token_address,
  cast(null as varchar(10)) as symbol, -- t.symbol, -- null until tokens_celo_erc721 spell is created
  sum(tr.amount_raw) as amount_raw,
  sum(tr.amount_raw / power(10, 18)) as amount --sum(tr.amount_raw / power(10, t.decimals)) as amount
from {{ ref('transfers_celo_erc721') }} tr
--left join ref('tokens_celo_erc721') t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where tr.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
group by 1, 2, 3, 4, 5, 6
