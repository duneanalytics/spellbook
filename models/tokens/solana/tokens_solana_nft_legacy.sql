{{ config(
    schema = 'tokens_solana_nft',
    tags = ['legacy'],
    alias = alias('nft', legacy_model=True),
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "tokens",
                                \'["ilemi"]\') }}')
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1