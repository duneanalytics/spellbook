{{ config(
    alias = 'optimism_attestationstation',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "optimism_attestationstation",
                                \'["chuxin"]\') }}'
    )
}}
select 
from {{source('optimism_attestationstation','OptimismAttestationStation_evt_AttestationSubmitted')}}
where true
{% if is_incremental() %}
and evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
