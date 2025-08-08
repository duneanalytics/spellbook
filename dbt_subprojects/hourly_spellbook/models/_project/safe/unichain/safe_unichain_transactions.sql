{{ 
    config(
        materialized='incremental',
        schema='safe_unichain',
        alias = 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'], 
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(blockchains =  \'["unichain"]\',
                                    spell_type = "project",
                                    spell_name = "safe",
                                    contributors = \'["tschubotz", "hosuke", "danielpartida", "safehjc"]\') }}'
    ) 
}}

{{ safe_transactions('unichain', '2025-01-29') }}
