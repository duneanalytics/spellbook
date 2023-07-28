{{ config(
    tags=['legacy'],
    alias = alias('accounts',legacy_model=True),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['account_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "worldcoin",
                                    \'["msilb7"]\') }}')}}

SELECT 1