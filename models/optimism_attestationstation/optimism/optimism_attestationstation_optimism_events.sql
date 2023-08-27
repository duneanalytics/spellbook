{{ config(
    alias = alias('events'),
    tags = ['dunesql'],
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
SELECT
  *
  , concat_ws(', ', val) AS val_string

  FROM (
    select 
        date_trunc('day', evt_block_time) as block_date
        ,evt_tx_hash as tx_hash
        ,evt_block_number as block_number
        ,evt_block_time as block_time
        ,evt_index
        ,about as recipient
        ,creator as issuer
        ,contract_address
        ,key as key_raw
        ,REGEXP_REPLACE(--Replace invisible characters
            from_utf8(key), '[^\x20-\x7E]','')
        as key
        ,val as val_raw

        ,split(
                REGEXP_REPLACE(--Replace invisible characters
                        CASE WHEN cast( REGEXP_REPLACE(from_utf8(val), '[^\x20-\x7E]','') as varchar(100)) != ''
                            THEN cast(from_utf8(val) as varchar(100))
                            WHEN bytearray_length(val) <= 32 then cast(bytearray_to_uint256(val) as varchar(100))
                        END  
                  , '[^\x20-\x7E]','')
              ,',') as val
            

        ,case when bytearray_length(val) <= 32 then bytearray_to_uint256(val) end AS val_byte2numeric

    from {{source('attestationstation_optimism','AttestationStation_evt_AttestationCreated')}}
    where 
        true
        {% if is_incremental() %}
        and evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
  ) a
