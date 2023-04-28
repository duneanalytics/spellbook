{{ config(
    schema = 'liquidifty',
    alias = 'events',
    post_hook = '{{ expose_spells(\'["bnb", "ethereum"]\',
                                    "project",
                                    "liquidifty",
                                    \'["bizzyvinci"]\') }}'
)}}

select * from {{ ref('liquidifty_bnb_events') }}
union all
select * from {{ ref('liquidifty_ethereum_events') }}
