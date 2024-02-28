{{ config(
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "platypus_finance_v1",
                                \'["umer_h_adil"]\') }}'
    )
}}

/*
    buyer
        gives fromAmount of fromToken
        receives toAmount of toToken
    seller (ie Platypus)
        gives toAmount of toToken,
        receives fromAmount of fromToken
    Platypus allows the sender & reciever to be different
    ie--
        a regular swap looks like--	sender <-> pool
        but Platypus allows--		sender -> pool -> receiver
        here, receiver and sender can be identical (resulting in a regular swap), but don't have to be
    As the receiver (ie the "to" address) ultimately receives the swapped tokens, we designate him/her as taker
*/

{% set project_start_date = '2021-11-26' %}

select
	'avalanche_c' as blockchain
	, 'platypus_finance' as project
	, '1' as version
	, cast(date_trunc('day', s.evt_block_time) as date) as block_date
    , cast(date_trunc('month', s.evt_block_time) AS date) as block_month
	, s.evt_block_time as block_time
	, erc20_b.symbol as token_bought_symbol
	, erc20_s.symbol as token_sold_symbol
	, case
        when lower(erc20_b.symbol) > lower(erc20_s.symbol) then concat(erc20_s.symbol, '-', erc20_b.symbol)
        else concat(erc20_b.symbol, '-', erc20_s.symbol)
    end as token_pair
	, s.toAmount / power(10, erc20_b.decimals) as token_bought_amount
	, s.fromAmount / power(10, erc20_s.decimals) as token_sold_amount
	, s.toAmount as token_bought_amount_raw
	, s.fromAmount as token_sold_amount_raw
    , coalesce(
        (s.toAmount / power(10, prices_b.decimals)) * prices_b.price
        ,(s.fromAmount / power(10, prices_s.decimals)) * prices_s.price
    ) as amount_usd
	, s.toToken as token_bought_address
	, s.fromToken as token_sold_address
    , coalesce(s."to", tx."from") AS taker
	, CAST(NULL AS VARBINARY) AS maker
	, s.contract_address as project_contract_address
	, s.evt_tx_hash as tx_hash
    , tx."from" AS tx_from
    , tx.to AS tx_to
	, s.evt_index as evt_index
from
    {{ source('platypus_finance_avalanche_c', 'Pool_evt_Swap') }} s
inner join {{ source('avalanche_c', 'transactions') }} tx
    ON tx.hash = s.evt_tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
-- bought tokens
left join {{ source('tokens', 'erc20') }} erc20_b
    on erc20_b.contract_address = s.toToken
    and erc20_b.blockchain = 'avalanche_c'
-- sold tokens
left join {{ source('tokens', 'erc20') }} erc20_s
    on erc20_s.contract_address = s.fromToken
    and erc20_s.blockchain = 'avalanche_c'
-- price of bought tokens
left join {{ source('prices', 'usd') }} prices_b
    on prices_b.minute = date_trunc('minute', s.evt_block_time)
    and prices_b.contract_address = s.toToken
    and prices_b.blockchain = 'avalanche_c'
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
    and prices_s.blockchain = 'avalanche_c'
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
