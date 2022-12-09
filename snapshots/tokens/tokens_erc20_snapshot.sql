{% snapshot tokens_erc20_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key="blockchain'||'-'||'contract_address",
      strategy='check',
      check_cols= 'all',
      invalidate_hard_deletes=True,
      depends_on = []
    )
}}

select * from {{ ref('tokens_erc20') }}

{% endsnapshot %}