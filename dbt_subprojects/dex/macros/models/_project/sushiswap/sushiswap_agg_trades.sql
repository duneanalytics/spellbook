{% macro generate_sushiswap_trades(chain) %}
  {% set fn_map = {
    'routeprocessor_call_processRoute'                         : '1',
    'routeprocessor_call_transferValueAndprocessRoute'         : '1',
    'routeprocessor2_call_processRoute'                        : '2',
    'routeprocessor2_call_transferValueAndprocessRoute'        : '2',
    'routeprocessor3_call_processRoute'                        : '3',
    'routeprocessor3_call_transferValueAndprocessRoute'        : '3',
    'routeprocessor3_1_call_processRoute'                      : '3.1',
    'routeprocessor3_2_call_processRoute'                      : '3.2',
    'routeprocessor3_2_call_transferValueAndprocessRoute'      : '3.2',
    'routeprocessor4_call_processRoute'                        : '4',
    'routeprocessor4_call_transferValueAndprocessRoute'        : '4',
    'routeprocessor5_call_processRoute'                        : '5',
    'routeprocessor5_call_transferValueAndprocessRoute'        : '5',
    'routeprocessor5_call_processRouteWithTransferValueOutput' : '5',
    'routeprocessor5_call_processRouteWithTransferValueInput'  : '5',
    'routeprocessor6_call_processRoute'                        : '6',
    'routeprocessor6_call_transferValueAndprocessRoute'        : '6',
    'routeprocessor6_call_processRouteWithTransferValueOutput' : '6',
    'routeprocessor6_call_processRouteWithTransferValueInput'  : '6',
    'routeprocessor6_1_call_processRoute'                      : '6.1',
    'routeprocessor6_1_call_transferValueAndprocessRoute'      : '6.1',
    'routeprocessor6_1_call_processRouteWithTransferValueOutput': '6.1',
    'routeprocessor6_1_call_processRouteWithTransferValueInput' : '6.1',
    'routeprocessor7_call_processRoute'                        : '7',
    'routeprocessor7_call_transferValueAndprocessRoute'        : '7',
    'routeprocessor7_call_processRouteWithTransferValueOutput' : '7',
    'routeprocessor7_call_processRouteWithTransferValueInput'  : '7',
  } %}

{% set versions_by_chain = {
  'apechain'    : ['6','6.1','7'],
  'arbitrum'    : ['1','2','3.1','4','5','6','6.1','7'],
  'avalanche_c' : ['1','2','4','5','6','6.1','7'],
  'base'        : ['3','3.1','3.2','4','5','6','6.1','7'],
  'blast'       : ['4','5','6','6.1','7'],
  'bnb'         : ['1','2','3','3.2','4','5','6','6.1','7'],
  'celo'        : ['4','5','6','6.1','7'],
  'ethereum'    : ['3.2','4','5','6','6.1','7'],
  'fantom'      : ['1','2','3.2','5','6','6.1','7'],
  'gnosis'      : ['1','2','3','3.2','4','5','6','6.1','7'],
  'katana'      : ['7'],
  'linea'       : ['3','4','5','6','6.1','7'],
  'mantle'      : ['5','6','6.1','7'],
  'nova'        : ['1','2','3','3.2','4','5','6','6.1','7'],
  'optimism'    : ['3.2','4','5','6','6.1','7'],
  'polygon'     : ['1','2','3','3.1','3.2','4','5','6','6.1','7'],
  'scroll'      : ['3.2','4','5','6','6.1','7'],
  'sonic'       : ['5','6','6.1','7'],
  'zkevm'       : ['3','3.2','4','5','6','6.1'],
  'zksync'      : ['5','6','6.1','7']
} %}

  {% set wrapped_native = {
    'ethereum' : '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
    'apechain' : '0x48b62137edfa95a428d35c09e44256a739f6b557',
    'arbitrum' : '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1',
    'avalanche_c' : '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7',
    'base' : '0x4200000000000000000000000000000000000006',
    'blast' : '0x4300000000000000000000000000000000000004',
    'bnb' : '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c',
    'celo' : '0x471EcE3750Da237f93B8E339c536989b8978a438',
    'ethereum' : '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
    'fantom' : '0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83',
    'gnosis' : '0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d',
    'katana' : '0xEE7D8BCFb72bC1880D0Cf19822eB0A2e6577aB62',
    'linea' : '0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f',
    'mantle' : '0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8',
    'nova' : '0x722E8BdD2ce80A4422E880164f2079488e115365',
    'optimism' : '0x4200000000000000000000000000000000000006',
    'polygon' : '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270',
    'scroll' : '0x5300000000000000000000000000000000000004',
    'sonic' : '0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38',
    'zkevm' : '0x4F9A0e7FD2Bf6067db6994CF12E4495Df938E6e9',
    'zksync' : '0x5AEa5775959fBC2557Cc8789bC1bf90A239D9a91'
  } %}

  {% set versions             = versions_by_chain[chain] %}
  {% set wrapped_native_token = wrapped_native[chain] %}
  {% set schema               = 'sushiswap_' ~ chain %}

  {#── filter fn_map → fns + version_map ──#}
  {% set fns         = [] %}
  {% set version_map = {} %}
  {% for fn, ver in fn_map.items() %}
    {% if ver in versions %}
      {% do fns.append(fn) %}
      {% do version_map.update({ fn: ver }) %}
    {% endif %}
  {% endfor %}

  with raw_calls as (
    {% for fn in fns %}
    select
      '{{ version_map[fn] }}' as version,
      call_block_time as block_time,
      call_block_number,
      call_tx_hash   as tx_hash,
      call_tx_index  as evt_index,
      call_trace_address as trace_address,
      call_tx_from   as tx_from,
      call_tx_to     as tx_to,
      '{{ fn }}' as method,
      tokenIn        as token_sold_address,
      tokenOut       as token_bought_address,
      amountIn       as token_sold_amount_raw,
      output_amountOut as token_bought_amount_raw,
      call_tx_to     as project_contract_address
    FROM {{ source('sushiswap_' ~ chain, fn )}} 
    where call_success = true
        {% if is_incremental() %}
        and {{incremental_predicate('call_block_time')}}
        {% endif %}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
  ),

  tokens_mapped as (
    select *,
      case
        when token_sold_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
        then {{ wrapped_native_token }}
        else token_sold_address
      end as token_sold_adjusted,
      case
        when token_bought_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
        then {{ wrapped_native_token }}
        else token_bought_address
      end as token_bought_adjusted
    from raw_calls
  ),

  
price_data as (
    select
        date_trunc('day', block_time) as block_date,
        date_trunc('month', block_time) as block_month,
        block_time AS block_time,
        '{{ chain }}' as blockchain,
        'sushiswap' as project,
        version,
        t_bought.symbol as token_bought_symbol,
        t_sold.symbol as token_sold_symbol,
        case
          when lower(t_bought.symbol) > lower(t_sold.symbol)
          then concat(t_sold.symbol,'-',t_bought.symbol)
          else concat(t_bought.symbol,'-',t_sold.symbol)
        end as token_pair,
        tm.token_bought_amount_raw / power(10, coalesce(t_bought.decimals,18)) as token_bought_amount,
        tm.token_sold_amount_raw / power(10, coalesce(t_sold.decimals,18)) as token_sold_amount,
        tm.token_bought_amount_raw,
        tm.token_sold_amount_raw,
        coalesce(
          (tm.token_bought_amount_raw / power(10, coalesce(t_bought.decimals,18))) * p_bought.price,
          (tm.token_sold_amount_raw / power(10, coalesce(t_sold.decimals,18))) * p_sold.price
        ) as amount_usd,
        tm.token_bought_adjusted as token_bought_address,
        tm.token_sold_adjusted as token_sold_address,
        tm.tx_from,
        tm.tx_to,
        tm.tx_from as taker,
        tm.tx_to as maker,
        tm.project_contract_address,
        tm.tx_hash,
        tm.method,
        CASE
          WHEN tm.trace_address IS NULL
            OR cardinality(tm.trace_address) = 0
          THEN cast(ARRAY[-1] AS array<bigint>)
          ELSE tm.trace_address
        END AS trace_address,
        CAST(-1 as integer) AS evt_index,
        'Single' as trade_type
    from tokens_mapped tm
    left join {{ source('tokens','erc20') }} t_bought on t_bought.contract_address = tm.token_bought_adjusted
      and t_bought.blockchain = '{{ chain }}'
    left join {{ source('tokens','erc20') }} t_sold on t_sold.contract_address = tm.token_sold_adjusted
      and t_sold.blockchain = '{{ chain }}'
    left join {{ source('prices','usd') }} p_bought
      on p_bought.contract_address = tm.token_bought_adjusted
      and p_bought.blockchain = '{{ chain }}'
      and p_bought.minute = date_trunc('minute', tm.block_time)
      {% if is_incremental() %}
      and {{incremental_predicate('p_bought.minute')}}
      {% endif %}
    left join {{ source('prices','usd') }} p_sold
      on p_sold.contract_address = tm.token_sold_adjusted
      and p_sold.blockchain = '{{ chain }}'
      and p_sold.minute = date_trunc('minute', tm.block_time)
      {% if is_incremental() %}
      and {{incremental_predicate('p_sold.minute')}}
      {% endif %}
)
select
  '{{ chain }}'    as blockchain,
  'sushiswap'      as project,
  version,
  method,
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
from price_data
  


{% endmacro %}
