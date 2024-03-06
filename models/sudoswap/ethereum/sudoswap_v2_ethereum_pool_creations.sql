{{ config(
        alias = 'pool_creations',
        schema = 'sudoswap_v2_ethereum',
        
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "sudoswap",
                                    \'["niftytable","0xRob"]\') }}'
        )
}}

with
  pool_creations as (
      SELECT 
          output_pair AS pool_address,
          nft_contract_address,
          nft_type,
          case when token = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 then 'ETH' else 'ERC20' end as token_type,
          token as token_contract_address,
          CASE
            WHEN bonding_curve = 0xe5d78fec1a7f42d2F3620238C498F088A866FdC5 THEN 'linear'
            WHEN bonding_curve = 0xfa056C602aD0C0C4EE4385b3233f2Cb06730334a THEN 'exponential'
            WHEN bonding_curve = 0xc7fB91B6cd3C67E02EC08013CEBb29b1241f3De5 THEN 'xyk'
            WHEN bonding_curve = 0x1fD5876d4A3860Eb0159055a3b7Cb79fdFFf6B67 then 'GDA'
            ELSE 'other'
          END as bonding_curve,
          CASE
            WHEN pool_type_raw = 0 THEN 'token'
            WHEN pool_type_raw = 1 THEN 'nft'
            WHEN pool_type_raw = 2 THEN 'trade'
          END AS pool_type,
          contract_address as pool_factory,
          call_block_time as creation_block_time,
          call_tx_hash as creation_tx_hash,
          tx."from" as creator_address
    FROM (
          SELECT 
              output_pair,
              _nft AS nft_contract_address,
              _bondingCurve as bonding_curve,
              _poolType as pool_type_raw,
              'ERC721' as nft_type,
              0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token,
              contract_address,
              call_block_time,
              call_tx_hash
        FROM {{ source('sudoswap_v2_ethereum','LSSVMPairFactory_call_createPairERC721ETH') }}
        WHERE 1=1
          {% if is_incremental() %}
          AND call_block_time >= date_trunc('day', now() - interval '7' day)
          {% endif %}
        UNION ALL
        SELECT
              output_pair,
              _nft AS nft_contract_address,
              _bondingCurve as bonding_curve,
              _poolType as pool_type_raw,
              'ERC1155' as nft_type,
              0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token,
              contract_address,
              call_block_time,
              call_tx_hash
        FROM {{ source('sudoswap_v2_ethereum','LSSVMPairFactory_call_createPairERC1155ETH') }}
        WHERE 1=1
          {% if is_incremental() %}
          AND call_block_time >= date_trunc('day', now() - interval '7' day)
          {% endif %}
        UNION ALL 
        SELECT 
            output_pair
            , from_hex(json_extract_scalar(params,'$.nft')) as nft_contract_address
            , from_hex(json_extract_scalar(params,'$.bondingCurve')) as bonding_curve
            , cast(json_extract_scalar(params,'$.poolType') as int) as pool_type_raw
            , 'ERC1155' as nft_type
            , from_hex(json_extract_scalar(params,'$.token')) as token_type
            , contract_address
            , call_block_time
            , call_tx_hash
        FROM {{ source('sudoswap_v2_ethereum','LSSVMPairFactory_call_createPairERC1155ERC20') }}
        WHERE 1=1
          {% if is_incremental() %}
          AND call_block_time >= date_trunc('day', now() - interval '7' day)
          {% endif %}
        UNION ALL 
        SELECT 
            output_pair
            , from_hex(json_extract_scalar(params,'$.nft')) as nft_contract_address
            , from_hex(json_extract_scalar(params,'$.bondingCurve')) as bonding_curve
            , cast(json_extract_scalar(params,'$.poolType') as int) as pool_type_raw
            , 'ERC721' as nft_type
            , from_hex(json_extract_scalar(params,'$.token')) as token_type
            , contract_address
            , call_block_time
            , call_tx_hash
        FROM {{ source('sudoswap_v2_ethereum','LSSVMPairFactory_call_createPairERC721ERC20') }}
        WHERE 1=1
          {% if is_incremental() %}
          AND call_block_time >= date_trunc('day', now() - interval '7' day)
          {% endif %}
    ) cre
    INNER JOIN {{ source('ethereum','transactions') }} tx 
      ON tx.block_time = cre.call_block_time
      AND tx.hash = cre.call_tx_hash
      AND tx.success
      {% if is_incremental() %}
      AND tx.block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
  )

SELECT * FROM pool_creations
