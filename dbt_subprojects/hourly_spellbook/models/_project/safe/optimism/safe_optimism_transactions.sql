{{ 
    config(
        materialized='incremental',
        schema='safe_optimism',
        alias = 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(blockchains = \'["optimism"]\',
                                    spell_type = "project",
                                    spell_name = "safe",
                                    contributors = \'["tschubotz", "hosuke", "danielpartida"]\') }}'
    )
}}

{{ safe_transactions('optimism', '2021-11-17') }}