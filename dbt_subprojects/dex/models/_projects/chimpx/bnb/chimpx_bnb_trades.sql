{{ config(
    schema = 'chimpx_bnb',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ hide_spells() }}'
) }}

/*
  ChimpX AI-powered DeFi Aggregator — BNB Chain
  Registry contract: 0x8327839597934e1490f90D06F2b0A549dFC7edeB
  Deployed block:    82131810
  Deployer:          0x3d11Bea8077816E2B55cb4830f11D194f610DbA6

  ChimpX is an AI agent that executes DeFi actions (swaps, bridges, lending,
  borrowing, staking, and perpetuals) for users on BNB Chain via natural language.
  The backend relayer calls registerVolume() after each successful action, emitting
  VolumeRegistered with the USD notional (18-decimal fixed-point).

  ActionType enum:
    0=Swap  1=Bridge  2=Lend  3=Borrow  4=Stake  5=Unstake  6=PerpsLong  7=PerpsShort
*/

{% set project_start_date = '2025-01-01' %}

WITH volume_events AS (
    SELECT
        evt_block_time                                      AS block_time
        ,'chimpx'                                          AS project
        ,'1'                                               AS version
        ,"user"                                            AS taker
        ,CAST(NULL AS varbinary)                           AS maker
        -- volumeUsd is stored with 18 decimals; amount_usd must be a plain DOUBLE
        ,CAST(volumeUsd AS DOUBLE) / 1e18                  AS amount_usd
        -- ChimpX routes across multiple protocols; individual token info is not
        -- available at registry level. Populated NULL to satisfy schema.
        ,CAST(NULL AS varbinary)                           AS token_bought_address
        ,CAST(NULL AS varbinary)                           AS token_sold_address
        ,CAST(NULL AS UINT256)                             AS token_bought_amount_raw
        ,CAST(NULL AS UINT256)                             AS token_sold_amount_raw
        ,contract_address                                  AS project_contract_address
        ,evt_tx_hash                                       AS tx_hash
        ,evt_tx_from                                       AS tx_from
        ,evt_tx_to                                         AS tx_to
        ,CAST(evt_index AS BIGINT)                        AS evt_index
        ,ARRAY[-1]                                         AS trace_address
        ,evt_block_number                                  AS block_number
        -- Deduplicate rows arising from Dune's BSC chain reorg handling
        ,ROW_NUMBER() OVER (
            PARTITION BY evt_tx_hash, evt_index
            ORDER BY evt_block_number
        )                                                  AS rn
    FROM {{ source('chimpx_bnb', 'ChimpXVolumeRegistry_evt_VolumeRegistered') }}
    WHERE evt_block_number >= 82131810
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% else %}
    AND evt_block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
)

SELECT
    'bnb'                                                           AS blockchain
    ,project
    ,version
    ,CAST(date_trunc('day',   block_time) AS DATE)                  AS block_date
    ,CAST(date_trunc('month', block_time) AS DATE)                  AS block_month
    ,block_time
    ,CAST(NULL AS VARCHAR)                                          AS token_bought_symbol
    ,CAST(NULL AS VARCHAR)                                          AS token_sold_symbol
    ,CAST(NULL AS VARCHAR)                                          AS token_pair
    ,CAST(NULL AS DOUBLE)                                           AS token_bought_amount
    ,CAST(NULL AS DOUBLE)                                           AS token_sold_amount
    ,token_bought_amount_raw
    ,token_sold_amount_raw
    ,amount_usd
    ,token_bought_address
    ,token_sold_address
    ,taker
    ,maker
    ,project_contract_address
    ,tx_hash
    ,tx_from
    ,tx_to
    ,evt_index
    ,trace_address
FROM volume_events
WHERE rn = 1
