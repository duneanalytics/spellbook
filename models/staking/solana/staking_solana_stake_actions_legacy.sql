{{ config(
    schema = 'tokens_solana_nft',
    tags = ['legacy'],
    alias = alias('stake_actions', legacy_model=True),
    materialized = 'table',
    file_format = 'delta'
    post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "staking",
                                \'["ilemi"]\') }}')
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1