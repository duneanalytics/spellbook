{{ config(
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "wombat_v1",
                                \'["umer_h_adil", "hosuke"]\') }}'
    )
}}

{% set project_start_date = '2022-04-18' %}
{% set wombat_bnb_swap_evt_tables = [
    source('wombat_bnb', 'Pool_evt_Swap')
    , source('wombat_bnb', 'HighCovRatioFeePool_evt_Swap')
    , source('wombat_bnb', 'DynamicPool_evt_Swap')
    , source('wombat_bnb', 'mWOM_Pool_evt_Swap')
    , source('wombat_bnb', 'qWOM_WOMPool_evt_Swap')
    , source('wombat_bnb', 'WMX_WOM_Pool_evt_Swap')
] %}

with wombat_swaps_all_pools as (
    {% for swap_evt_table in wombat_bnb_swap_evt_tables %}
        select  toAmount
                , fromAmount
                , toToken
                , fromToken
                , evt_block_time
                , evt_tx_hash
                , evt_index
                , t.to
                , contract_address
        from {{ swap_evt_table }} t
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        {% if not loop.last %}
        union all
        {% endif %}

    {% endfor%}
)

select
	'bnb' as blockchain
	, 'wombat' as project
	, '1' as version
	, date_trunc('DAY', s.evt_block_time) as block_date
    , CAST(date_trunc('month', s.evt_block_time) AS DATE) as block_month
	, s.evt_block_time as block_time
    , s.toAmount  AS token_bought_amount_raw
    , s.fromAmount AS token_sold_amount_raw
    , coalesce(
        (s.toAmount / power(10, prices_b.decimals)) * prices_b.price
        ,(s.fromAmount / power(10, prices_s.decimals)) * prices_s.price
    ) as amount_usd
	, s.toToken as token_bought_address
	, s.fromToken as token_sold_address
	, erc20_b.symbol as token_bought_symbol
	, erc20_s.symbol as token_sold_symbol
	, case
        when lower(erc20_b.symbol) > lower(erc20_s.symbol) then concat(erc20_s.symbol, '-', erc20_b.symbol)
        else concat(erc20_b.symbol, '-', erc20_s.symbol)
    end as token_pair
	, s.toAmount / power(10, erc20_b.decimals) as token_bought_amount
	, s.fromAmount / power(10, erc20_s.decimals) as token_sold_amount
    , coalesce(s.to, tx."from") AS taker
	, CAST(NULL AS VARBINARY) AS maker
	, s.contract_address as project_contract_address
	, s.evt_tx_hash as tx_hash
    , tx."from" as tx_from
    , tx.to as tx_to
	, s.evt_index as evt_index
from
    wombat_swaps_all_pools s
inner join {{ source('bnb', 'transactions') }} tx
    on tx.hash = s.evt_tx_hash
    {% if not is_incremental() %}
    and tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
-- bought tokens
left join {{ source('tokens', 'erc20') }} erc20_b
    on erc20_b.contract_address = s.toToken
    and erc20_b.blockchain = 'bnb'
-- sold tokens
left join {{ source('tokens', 'erc20') }} erc20_s
    on erc20_s.contract_address = s.fromToken
    and erc20_s.blockchain = 'bnb'
-- price of bought tokens
left join {{ source('prices', 'usd') }} prices_b
    on prices_b.minute = date_trunc('minute', s.evt_block_time)
    and prices_b.contract_address = s.toToken
    and prices_b.blockchain = 'bnb'
	{% if not is_incremental() %}
    and prices_b.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and prices_b.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
-- price of sold tokens
left join {{ source('prices', 'usd') }} prices_s
    on prices_s.minute = date_trunc('minute', s.evt_block_time)
    and prices_s.contract_address = s.fromToken
    and prices_s.blockchain = 'bnb'
	{% if not is_incremental() %}
    and prices_s.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and prices_s.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
where 1 = 1
    {% if is_incremental() %}
    and s.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
