{{ config(
    schema = 'magiceden_solana',
    tags = ['legacy'],
    alias = alias('trades', legacy_model=True),
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "magiceden",
                                \'["ilemi"]\') }}')
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1