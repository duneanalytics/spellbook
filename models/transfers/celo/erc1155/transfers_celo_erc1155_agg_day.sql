{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc1155_agg_day'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['wallet_address', 'token_address', 'block_day', 'token_id'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago"]\') }}'
    )
}}

select
  'celo' as blockchain,
  cast(date_trunc('month', tr.block_time) as date) as block_month,
  date_trunc('day', tr.block_time) as block_day,
  tr.wallet_address,
  tr.token_address,
  tr.token_id,
  sum(tr.amount) as amount
from {{ ref('transfers_celo_erc1155') }} tr
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where tr.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
group by 1, 2, 3, 4, 5, 6
