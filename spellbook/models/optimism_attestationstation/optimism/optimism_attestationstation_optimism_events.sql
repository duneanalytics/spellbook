{{ config(
    alias = 'events',
    
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
        cast(date_trunc('day', evt_block_time) as date) as block_date
        ,evt_tx_hash as tx_hash
        ,evt_block_number as block_number
        ,evt_block_time as block_time
        ,evt_index
        ,about as recipient
        ,creator as issuer
        ,contract_address
        ,key as key_raw
        ,regexp_replace(
          -- Replace invisible characters
          from_utf8(
              from_hex(
                  CASE 
                      WHEN bytearray_substring(key, 1, 2) IN (0xab7e, 0x9e43) -- Handle for Clique
                      THEN to_hex(key)
                      ELSE substring(cast(key as varchar), 3)
                  END
              )
          ),
          '[^\x20-\x7E]',
          ''
        ) as key
        ,val as val_raw

        ,split(
                REGEXP_REPLACE(--Replace invisible characters
                        CASE WHEN cast( REGEXP_REPLACE(from_utf8(val), '[^\x20-\x7E]','') as varchar) != ''
                            THEN cast(from_utf8(val) as varchar)
                            ELSE cast(bytearray_to_uint256(
                              if(bytearray_length(val) > 32, bytearray_substring(val, 1, 32), val)) as varchar)
                        END  
                  , '[^\x20-\x7E]','')
              ,',') as val
            

        ,bytearray_to_uint256(
          if(bytearray_length(val) > 32, bytearray_substring(val, 1, 32), val)
        ) AS val_byte2numeric

    from {{source('attestationstation_optimism','AttestationStation_evt_AttestationCreated')}}
    where 
        true
        {% if is_incremental() %}
        and evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
  ) a
