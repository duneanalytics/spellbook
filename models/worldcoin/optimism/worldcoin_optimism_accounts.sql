{{ config(
    
    alias = 'accounts',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['account_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "worldcoin",
                                    \'["msilb7"]\') }}')}}

-- Addresses Sourced from https://dune.com/queries/2456211

SELECT
    created_tx_from AS worldcoin_deployer_address,
    contract_address AS account_address,
    created_time,
    created_tx_hash,
    created_block_number,
    contract_project,
    contract_name,
    trace_creator_address


FROM {{ ref('contracts_optimism_contract_mapping') }}
where 1=1 -- limit by time
    and created_time > TIMESTAMP '2023-06-01'
    {% if is_incremental() %}
    AND created_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    and created_tx_from IN (
        -- relayers (user tx)
          0x65bf36d6499a504100eb504f0719271f5c4174ec
        , 0xabe494eaa4ed80de8583c49183e9cbdadbc3b954
        , 0xb54a5205ee454f48ddfc23ca26a3836ba3dacc07
        , 0x4399fa85585f90da110d5ba150ff96c763bc0aba
        , 0xd8f7d2d62514895475afe0c7d75f31390dd40de4
        , 0x67Fa7f957F13e64c986ce1c7ef75De5c716E2367
        -- deployer wallets (safe deployments)
        , 0x91a3060513f25b44bacb46762160f6794bbd42d2
        , 0xe74E592E1dD43ccfb55bcd7999DEe0dfeE7acae3
        , 0x279A5453597E07505d574233fd16fbc670838fe7
        , 0x71730945b56A1472874EDf3E40795D74EF350416
        , 0x85E38523A65a5c9F628c5477033C742da1c218cc
        , 0x36C334Bd446ad62e602672828D21029ab48DD27F
        , 0x1d234d6D83B0535Ce28D11BBBBbC9426868D4Cb9
        , 0x9F4665f0ce1a377abDbD638F81F0C5883fbedCDb
        , 0x77Db88C013D5149d994B17F6586Cd71c3FFD2503
        , 0xc5a53b78734284D683d623C5C06D2e646abCec3f
        )