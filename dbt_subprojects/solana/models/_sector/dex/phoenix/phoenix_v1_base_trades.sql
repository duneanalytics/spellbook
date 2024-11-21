 {{
  config(
    schema = 'phoenix_v1',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month'],
    pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2023-02-15' %} --grabbed program deployed at time (account created at)

  WITH
  market_metadata as (
      --you can check phoenix_v1_call_InitializeMarket for this data, our decoding just has some nulls/incompletes so recreating manually.
      SELECT
          *
      FROM (
          VALUES
              ('4DoNfFBfF7UokCC2FQzriy7yHK6DY6NVdYpuekQ5pRgg', 'So11111111111111111111111111111111111111112', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 1),
              ('FZRgpfpvicJ3p23DfmZuvUgcQZBHJsWScTf2N2jK8dy6', 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So', 'So11111111111111111111111111111111111111112', 1),
              ('GBMoNx84HsFdVK63t8BZuDgyZhSBaeKWB4pHHpoeRM9z', 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 1000000),
              ('FicF181nDsEcasznMTPp9aLa5Rbpdtd11GtSEa1UUWzx', 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263', 'So11111111111111111111111111111111111111112', 1000000),
              ('2t9TBYyUyovhHQq434uAiBxW6DmJCg7w4xdDoSK6LRjP', 'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn', 'So11111111111111111111111111111111111111112', 1),
              ('Ew3vFDdtdGrknJAVVfraxCA37uNJtimXYPY4QjnfhFHH', '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 1),
              ('2sTMN9A1D1qeZLF95XQgJCUPiKe5DiV52jLfZGqMP46m', 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 1),
              ('BRLLmdtPGuuFn3BU6orYw4KHaohAEptBToi3dwRUnHQZ', 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 1),
              ('5x91Aaegvx1JmW7g8gDfWqwb6kPF7CdNunqNoYCdLjk1', 'HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 1),
              ('6ojSigXF7nDPyhFRgmn3V9ywhYseKF9J32ZrranMGVSX', 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 1),
              ('3J9LfemPBLowAJgpG3YdYPB9n6pUk7HEjwgS6Y5ToSFg', 'So11111111111111111111111111111111111111112', 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 1),
              ('2jxpfobdZDU3z9MsDCjAz8psSaTb5HPoDEtusFLGrPnD', 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263', 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 1000000),
              ('5LQLfGtqcC5rm2WuGxJf4tjqYmDjsQAbKo2AMLQ8KB7p', 'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 1)
      ) AS t (market_id, base_mint, quote_mint, raw_base_units_per_base_unit)
  )

  , pools as (
    SELECT
        length(json_extract_scalar(initializeParams, '$.InitializeParams.numBaseLotsPerBaseUnit')) - 1 as tokenA_decimals --if lot size is 1000, then its 3 decimals
        , ip.account_baseMint as tokenA
        , ip.account_baseVault as tokenAVault
        , length(json_extract_scalar(initializeParams, '$.InitializeParams.numQuoteLotsPerQuoteUnit')) - 1 as tokenB_decimals
        , ip.account_quoteMint as tokenB
        , ip.account_quoteVault as tokenBVault
        , cast(json_extract_scalar(initializeParams, '$.InitializeParams.takerFeeBps') as double)/100 as fee_tier
        , ip.account_market as pool_id
        , ip.call_tx_id as init_tx
    FROM {{ source('phoenix_v1_solana','phoenix_v1_call_InitializeMarket') }} ip
  )

  , logs AS (
    SELECT
      call_tx_id,
      call_is_inner,
      call_block_slot,
      call_block_time,
      call_inner_instruction_index,
      call_outer_instruction_index,
      call_tx_index,
      BYTEARRAY_TO_BIGINT (
        BYTEARRAY_REVERSE (BYTEARRAY_SUBSTRING (l.call_data, 4, 8))
      ) AS seq,
      TO_BASE58 ((BYTEARRAY_SUBSTRING (l.call_data, 28, 32))) AS market,
      cast(BYTEARRAY_TO_BIGINT (
        BYTEARRAY_REVERSE (
          BYTEARRAY_SUBSTRING (
            l.call_data,
            BYTEARRAY_LENGTH (l.call_data) - 23,
            8
          )
        )
      ) as uint256) AS tokenA_filled,
      l.call_outer_instruction_index AS index,
      cast(BYTEARRAY_TO_BIGINT (
        BYTEARRAY_REVERSE (
          BYTEARRAY_SUBSTRING (
            l.call_data,
            BYTEARRAY_LENGTH (l.call_data) - 15,
            8
          )
        )
      ) as uint256) AS tokenB_filled
    FROM
      {{ source('phoenix_v1_solana','phoenix_v1_call_Log')}} AS l
    WHERE 1=1
      --filter for 0 events
      and bytearray_length (l.call_data) > 93
      --instruction is swap
      and BYTEARRAY_TO_BIGINT (
        BYTEARRAY_REVERSE (BYTEARRAY_SUBSTRING (l.call_data, 3, 1))
      ) = 0
      --filter for trade size > 0
      AND BYTEARRAY_TO_BIGINT (
        BYTEARRAY_REVERSE (
          BYTEARRAY_SUBSTRING (
            l.call_data,
            BYTEARRAY_LENGTH (l.call_data) - 23,
            8
          )
        )
      ) > 0
      --filter for event FillSummary
      AND BYTEARRAY_TO_BIGINT (
        BYTEARRAY_REVERSE (
          BYTEARRAY_SUBSTRING (
            l.call_data,
            BYTEARRAY_LENGTH (l.call_data) - 42,
            1
          )
        )
      ) = 6
      {% if is_incremental() %}
      AND {{incremental_predicate('l.call_block_time')}}
      {% endif %}
      -- AND call_block_time >= now() - interval '7' day --qa
  ),
  max_log_index AS (
    SELECT
      market,
      seq,
      MAX(index) AS index
    FROM
      logs
    GROUP BY
      market,
      seq
  ),
  filtered_logs AS (
    SELECT
      l.*
    FROM
      logs AS l
      JOIN max_log_index AS m ON l.market = m.market
      AND l.seq = m.seq
      AND l.index = m.index
  ),
  trades as (
    SELECT
        l.call_block_time as block_time
        , 'phoenix' as project
        , 1 as version
        , 'solana' as blockchain
        , l.call_block_slot as block_slot
        , case when s.call_outer_executing_account = 'PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY' then 'direct'
            else s.call_outer_executing_account
            end as trade_source
        , case when s.side = 1 then l.tokenB_filled
            else (l.tokenA_filled*coalesce(mm.raw_base_units_per_base_unit,1)) --base unit can be adjusted by phoenix, i.e. for BONK it starts at 1e6. There is a script for updating the markets seed file.
            end as token_bought_amount_raw
        , case when s.side = 1 then (l.tokenA_filled*coalesce(mm.raw_base_units_per_base_unit,1))
            else l.tokenB_filled
            end as token_sold_amount_raw
        , p.pool_id
        , s.call_tx_signer as trader_id
        , s.call_tx_id as tx_id
        , s.call_outer_instruction_index as outer_instruction_index
        , COALESCE(s.call_inner_instruction_index,0) as inner_instruction_index
        , s.call_tx_index as tx_index
        , case when s.side = 1 then p.tokenB
            else p.tokenA
            end as token_bought_mint_address
        , case when s.side = 1 then p.tokenBVault
            else p.tokenAVault
            end as token_bought_vault
        , case when s.side = 1 then p.tokenA
            else p.tokenB
            end as token_sold_mint_address
        , case when s.side = 1 then p.tokenAVault
            else p.tokenBVault
            end as token_sold_vault
        , p.fee_tier
        , row_number() over (partition by seq order by COALESCE(s.call_inner_instruction_index, 0) desc) as recent_swap -- this ties the log to only the most recent swap call
    FROM filtered_logs l
    LEFT JOIN (
        SELECT
            *
            , 2 * bytearray_to_integer (bytearray_substring (call_data, 3, 1)) - 1 as side --if side = 1 then tokenB was bought, else tokenA was bought
        FROM {{ source('phoenix_v1_solana','phoenix_v1_call_Swap') }}
        WHERE 1=1
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% endif %}
        -- AND call_block_time >= now() - interval '7' day --qa
    ) s ON s.call_block_slot = l.call_block_slot
        AND s.call_tx_id = l.call_tx_id
        AND s.account_market = l.market
        AND s.call_outer_instruction_index = l.call_outer_instruction_index
        AND COALESCE(s.call_inner_instruction_index, 0) <= COALESCE(l.call_inner_instruction_index,0) --only get swaps before the log call
    JOIN pools p ON l.market = p.pool_id
    LEFT JOIN market_metadata mm ON l.market = mm.market_id
  )

SELECT
    tb.blockchain
    , tb.project
    , tb.version
    , CAST(date_trunc('month', tb.block_time) AS DATE) as block_month
    , tb.block_time
    , tb.block_slot
    , tb.trade_source
    , tb.token_bought_amount_raw
    , tb.token_sold_amount_raw
    , tb.fee_tier as fee_tier
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.pool_id as project_program_id
    , 'PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY' as project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
FROM trades tb
WHERE 1=1
AND recent_swap = 1
