{{ config(
    schema = 'sushiswap_optimism',
    alias = 'sushiswap_agg_optimism_trades',
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = [
      'block_date',
      'blockchain',
      'project',
      'version',
      'tx_hash',
      'evt_index',
      'trace_address'
    ],
    tags = ['optimism','sushiswap','trades','dex','aggregator'],
    incremental_predicates = [ incremental_predicate('call_block_time') ]
) }}

{% 
   set fns = [
     'routeprocessor3_2_call_processRoute',
     'routeprocessor3_2_call_transferValueAndprocessRoute',
     'routeprocessor4_call_processRoute',
     'routeprocessor4_call_transferValueAndprocessRoute',
     'routeprocessor5_call_processRoute',
     'routeprocessor5_call_transferValueAndprocessRoute',
     'routeprocessor5_call_processRouteWithTransferValueOutput',
     'routeprocessor5_call_processRouteWithTransferValueInput',
     'routeprocessor6_call_processRoute',
     'routeprocessor6_call_transferValueAndprocessRoute',
     'routeprocessor6_call_processRouteWithTransferValueOutput',
     'routeprocessor6_call_processRouteWithTransferValueInput',
     'routeprocessor6_1_call_processRoute',
     'routeprocessor6_1_call_transferValueAndprocessRoute',
     'routeprocessor6_1_call_processRouteWithTransferValueOutput',
     'routeprocessor6_1_call_processRouteWithTransferValueInput',
     'routeprocessor7_call_processRoute',
     'routeprocessor7_call_transferValueAndprocessRoute',
     'routeprocessor7_call_processRouteWithTransferValueOutput',
     'routeprocessor7_call_processRouteWithTransferValueInput'
   ]
%}
{% set version_map = {
  'routeprocessor3_2_call_processRoute': '3',
  'routeprocessor3_2_call_transferValueAndprocessRoute': '3',
  'routeprocessor4_call_processRoute': '4',
  'routeprocessor4_call_transferValueAndprocessRoute': '4',
  'routeprocessor5_call_processRoute': '5',
  'routeprocessor5_call_transferValueAndprocessRoute': '5',
  'routeprocessor5_call_processRouteWithTransferValueOutput': '5',
  'routeprocessor5_call_processRouteWithTransferValueInput': '5',
  'routeprocessor6_call_processRoute': '6',
  'routeprocessor6_call_transferValueAndprocessRoute': '6',
  'routeprocessor6_call_processRouteWithTransferValueOutput': '6',
  'routeprocessor6_call_processRouteWithTransferValueInput': '6',
  'routeprocessor6_1_call_processRoute': '6',
  'routeprocessor6_1_call_transferValueAndprocessRoute': '6',
  'routeprocessor6_1_call_processRouteWithTransferValueOutput': '6',
  'routeprocessor6_1_call_processRouteWithTransferValueInput': '6',
  'routeprocessor7_call_processRoute': '7',
  'routeprocessor7_call_transferValueAndprocessRoute': '7',
  'routeprocessor7_call_processRouteWithTransferValueOutput': '7',
  'routeprocessor7_call_processRouteWithTransferValueInput': '7'
} %}
 

WITH raw_calls AS (

  {% for fn in fns %}
  SELECT
    '{{ version_map[fn] }}' AS version,
    call_block_time,
    call_block_number,
    call_tx_hash AS tx_hash,
    call_tx_index AS evt_index,
    call_trace_address AS trace_address,
    call_tx_from AS tx_from,
    call_tx_to AS tx_to,
    tokenIn AS token_sold_address,
    tokenOut AS token_bought_address,
    amountIn AS token_sold_amount_raw,
    output_amountOut AS token_bought_amount_raw,
    call_tx_to AS project_contract_address
  FROM sushiswap_optimism.{{ fn }}
  
  WHERE call_success = TRUE
    {% if is_incremental() %}
      AND {{ incremental_predicate('call_block_time') }}
    {% endif %}
  {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}

),

tokens_mapped AS (
  SELECT
    *,
    CASE
      WHEN token_sold_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
      THEN 0x4200000000000000000000000000000000000006
      ELSE token_sold_address
    END AS token_sold_adjusted,
    CASE
      WHEN token_bought_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
      THEN 0x4200000000000000000000000000000000000006
      ELSE token_bought_address
    END AS token_bought_adjusted
  FROM raw_calls
),

price_data AS (
  SELECT
    date_trunc('day', call_block_time) AS block_date,
    date_trunc('month', call_block_time) AS block_month,
    call_block_time  AS block_time,
    'optimism' AS blockchain,
    'sushiswap' AS project,
    version,
    t_bought.symbol AS token_bought_symbol,
    t_sold.symbol AS token_sold_symbol,
    CASE 
      WHEN lower(t_bought.symbol) > lower(t_sold.symbol)
      THEN concat(t_sold.symbol,'-',t_bought.symbol)
      ELSE concat(t_bought.symbol,'-',t_sold.symbol)
    END AS token_pair,
    tm.token_bought_amount_raw / power(10, coalesce(t_bought.decimals,0)) AS token_bought_amount,
    tm.token_sold_amount_raw   / power(10, coalesce(t_sold.decimals,0))   AS token_sold_amount,
    tm.token_bought_amount_raw AS token_bought_amount_raw,
    tm.token_sold_amount_raw   AS token_sold_amount_raw,
    COALESCE(
      (tm.token_bought_amount_raw / power(10, coalesce(t_bought.decimals,0))) * p_bought.price,
      (tm.token_sold_amount_raw   / power(10, coalesce(t_sold.decimals,0)))   * p_sold.price
    ) AS amount_usd,
    tm.token_bought_adjusted AS token_bought_address,
    tm.token_sold_adjusted   AS token_sold_address,
    tm.tx_from,
    tm.tx_to,
    tm.tx_from AS taker,
    tm.tx_to AS maker,
    tm.project_contract_address,
    tm.tx_hash,
    tm.trace_address,
    tm.evt_index,
    'Single' AS trade_type
  FROM tokens_mapped tm

  LEFT JOIN {{ source('tokens','erc20') }} t_bought
    ON t_bought.contract_address = tm.token_bought_adjusted
    AND t_bought.blockchain = 'optimism'

  LEFT JOIN {{ source('tokens','erc20') }} t_sold
    ON t_sold.contract_address = tm.token_sold_adjusted
    AND t_sold.blockchain = 'optimism'

  LEFT JOIN {{ source('prices','usd') }} p_bought
    ON p_bought.contract_address = tm.token_bought_adjusted
    AND p_bought.blockchain = 'optimism'
    AND p_bought.minute = date_trunc('minute', tm.call_block_time)
    {% if is_incremental() %} AND {{ incremental_predicate('p_bought.minute') }}{% endif %}

  LEFT JOIN {{ source('prices','usd') }} p_sold
    ON p_sold.contract_address = tm.token_sold_adjusted
    AND p_sold.blockchain = 'optimism'
    AND p_sold.minute = date_trunc('minute', tm.call_block_time)
    {% if is_incremental() %} AND {{ incremental_predicate('p_sold.minute') }}{% endif %}
)

SELECT
  blockchain,
  project,
  version,
  block_date,
  block_time,
  token_bought_symbol,
  token_sold_symbol,
  token_pair,
  token_bought_amount,
  token_sold_amount,
  token_bought_amount_raw,
  token_sold_amount_raw,
  amount_usd,
  token_bought_address,
  token_sold_address,
  tx_from,
  tx_to,
  taker,
  maker,
  project_contract_address,
  tx_hash,
  trace_address,
  evt_index,
  block_month,
  trade_type
FROM price_data;