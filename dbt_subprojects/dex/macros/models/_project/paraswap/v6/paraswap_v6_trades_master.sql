{% macro paraswap_v6_trades_by_contract(blockchain, project, contract_name, contract_details) %}
 with v6_trades as (    
    with
      sell_trades as (
            with
            swapExactAmountIn as ({{ paraswap_v6_uniswaplike_method( source(project + '_' + blockchain, contract_name + '_call_swapExactAmountIn'), 'swapExactAmountIn', 'swapData') }}),
          swapExactAmountInOnUniswapV2 as ({{ paraswap_v6_uniswaplike_method( source(project + '_' + blockchain, contract_name + '_call_swapExactAmountInOnUniswapV2'), 'swapExactAmountInOnUniswapV2', 'uniData') }}),
          swapExactAmountInOnUniswapV3 as ({{ paraswap_v6_uniswaplike_method( source(project + '_' + blockchain, contract_name + '_call_swapExactAmountInOnUniswapV3'), 'swapExactAmountInOnUniswapV3', 'uniData') }}),
          swapExactAmountInOnCurveV1 as ({{ paraswap_v6_uniswaplike_method( source(project + '_' + blockchain, contract_name + '_call_swapExactAmountInOnCurveV1'), 'swapExactAmountInOnCurveV1', 'curveV1Data') }}),
          swapExactAmountInOnCurveV2 as ({{ paraswap_v6_uniswaplike_method( source(project + '_' + blockchain, contract_name + '_call_swapExactAmountInOnCurveV2'), 'swapExactAmountInOnCurveV2', 'curveV2Data') }}),
          swapExactAmountInOnBalancerV2 as ({{ paraswap_v6_balancer_v2_method('swapExactAmountInOnBalancerV2_decoded', 'swapExactAmountInOnBalancerV2_raw', source(project + '_' + blockchain, contract_name + '_call_swapExactAmountInOnBalancerV2'), 'in', 'swapExactAmountInOnBalancerV2') }})
          -- TODO: should be possible to improve this conditional code
          {% if contract_details['version'] == '6.2' %}
               ,swapOnAugustusRFQTryBatchFill as ({{ paraswap_v6_rfq_method( source(project + '_' + blockchain, contract_name + '_call_swapOnAugustusRFQTryBatchFill')) }}) -- RFQ - not distinguishing between buy/sell
               ,swapExactAmountInOutOnMakerPSM as ({{ paraswap_v6_maker_psm_method( source(project + '_' + blockchain, contract_name + '_call_swapExactAmountInOutOnMakerPSM')) }}) -- Maker PSM - not distinguishing between buy/sell
          {% endif %}

select
  *,
  fromAmount as spentAmount,
  'sell' as side
from
          (
            select
              *
            from
              swapExactAmountIn
            union
            select
              *
            from
              swapExactAmountInOnUniswapV2
            union
            select
              *
            from
              swapExactAmountInOnUniswapV3
            union
            select
              *
            from
              swapExactAmountInOnCurveV1
            union
            select
              *
            from
              swapExactAmountInOnCurveV2
            union
            select
              *
            from
              swapExactAmountInOnBalancerV2
            -- TODO: should be possible to improve this conditional code
            {% if contract_details['version'] == '6.2' %}
            union select * from swapOnAugustusRFQTryBatchFill
            union select * from swapExactAmountInOutOnMakerPSM
            {% endif %}
          )
      ),
      buy_trades as (
            with            
              swapExactAmountOut as ({{ paraswap_v6_uniswaplike_method( source(project + '_' + blockchain, contract_name + '_call_swapExactAmountOut'), 'swapExactAmountOut', 'swapData', 'output_spentAmount as spentAmount') }}),
              swapExactAmountOutOnUniswapV2 as ({{ paraswap_v6_uniswaplike_method( source(project + '_' + blockchain, contract_name + '_call_swapExactAmountOutOnUniswapV2'), 'swapExactAmountOutOnUniswapV2', 'uniData', 'output_spentAmount as spentAmount' ) }}),
              swapExactAmountOutOnUniswapV3 as ({{ paraswap_v6_uniswaplike_method( source(project + '_' + blockchain, contract_name + '_call_swapExactAmountOutOnUniswapV3'), 'swapExactAmountOutOnUniswapV3', 'uniData', 'output_spentAmount as spentAmount') }}),
              swapExactAmountOutOnBalancerV2 as ({{ paraswap_v6_balancer_v2_method('swapexactAmountOutOnBalancerV2_decoded', 'swapexactAmountOutOnBalancerV2_raw', source(project + '_' + blockchain, contract_name + '_call_swapExactAmountOutOnBalancerV2'), 'out', 'swapExactAmountOutOnBalancerV2')}} )              
            select
              *,
              'buy' as side
        from
          (
            select
              *
            from
              swapExactAmountOut
            union
            select
              *
            from
              swapExactAmountOutOnUniswapV2
            union
            select
              *
            from
              swapExactAmountOutOnUniswapV3
            union
            select
              *
            from
              swapExactAmountOutOnBalancerV2
          )
      )
    select
      *
    from
      (
        select
          *
        from
          sell_trades
        union
        select
          *
        from
          buy_trades
      )
  )
select
  '{{ blockchain }}' as blockchain,
  cast(date_trunc('day', call_block_time) as date) as block_date,
  cast(date_trunc('month', call_block_time) as date) as block_month,
  'paraswap' AS project,
  '{{ contract_details['version'] }}' as version,
  call_block_time as blockTime,
  call_block_number as blockNumber,
  call_tx_hash as call_tx_hash,
  project_contract_address as projectContractAddress,
  call_trace_address as call_trace_address,
  srcToken,
  destToken,
  fromAmount,
  spentAmount,
  toAmount,
  quotedAmount,
  output_receivedAmount as receivedAmount,
  metadata,
  beneficiary,
  method,
  side,
  partnerAndFee as feeCode,
  output_partnerShare as partnerShare,
  output_paraswapShare as paraswapShare,
  '0x' || regexp_replace(
                try_cast(
                  TRY_CAST(
                    BITWISE_RIGHT_SHIFT(partnerAndFee, 96) AS VARBINARY
                  ) as VARCHAR
                ),
                '0x(00){12}'
              ) AS partnerAddress,
              BITWISE_AND(
                try_cast(partnerAndFee as UINT256),
                varbinary_to_uint256 (0x3FFF)
              ) as feeBps,              
              BITWISE_AND(
                try_cast(partnerAndFee as uint256),
                bitwise_left_shift(TRY_CAST(1 as uint256), 94)
              ) <> 0 AS isReferral,
              BITWISE_AND(
                try_cast(partnerAndFee as uint256),
                bitwise_left_shift(TRY_CAST(1 as uint256), 95)
              ) <> 0 AS isTakeSurplus
  from 
    v6_trades{% endmacro %}

{% macro paraswap_v6_trades_master(blockchain, project) %}
  {% 
    set contracts = {
          "AugustusV6_1": {
              "version":    "6.1",
          },
          "AugustusV6_2": {
              "version":    "6.2",              
          }
    } 
  %}
  {% for contract_name, contract_details in contracts.items() %}  
    select * from ({{ paraswap_v6_trades_by_contract(blockchain, project, contract_name, contract_details) }})    
    {% if not loop.last %}
      union all
    {% endif %}
  {% endfor %}
{% endmacro %}