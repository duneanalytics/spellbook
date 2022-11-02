{{ config(
    schema = 'platypus_finance',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "platypus_finance_v1",
                                \'["umer_h_adil"]\') }}'
    )
}}


{% set project_start_date = '2021-11-26' %}

select
	'avalanche_c' as blockchain
	, 'platypus_finance' as project
	, 1 as version
	, date_trunc('DAY', s.evt_block_time) as block_date
	, s.evt_block_time as block_time
	-- buyer
	--		gives fromAmount of fromToken
	--		receives toAmount of toToken
	-- seller (ie Platypus)
	--		gives toAmount of toToken,
	--		receives fromAmount of fromToken
	, s.toAmount as token_bought_amount_raw
	, s.fromAmount as token_sold_amount_raw
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
	-- Platypus allows the sender & reciever to be different
	-- ie:
	--		a regular swap looks like:	sender <-> pool
	-- 		but Platypus allows:		sender -> pool -> receiver
	--			here, receiver and sender can be identical (resulting in a regular swap), but don't have to be
	-- As the receiver (ie the `to` address) ultimately receives the swapped tokens, we designate him/her as taker
	, s.`to` as taker
	, '' as maker
	, cast(s.contract_address as string) as project_contract_address
	, s.evt_tx_hash as tx_hash
	, s.sender as tx_from
	, s.`to` as tx_to
	, '' as trace_address
	, s.evt_index as evt_index
from 
    {{ source('platypus_finance_avalanche_c', 'Pool_evt_Swap') }} s
-- bought tokens
left join {{ ref('tokens_erc20') }} erc20_b
    on erc20_b.contract_address = s.toToken 
    and erc20_b.blockchain = 'avalanche_c'
-- sold tokens
left join {{ ref('tokens_erc20') }} erc20_s
    on erc20_s.contract_address = s.fromToken
    and erc20_s.blockchain = 'avalanche_c'
-- price of bought tokens
left join {{ source('prices', 'usd') }} prices_b
    on prices_b.minute = date_trunc('minute', s.evt_block_time)
    and prices_b.contract_address = s.toToken
    and prices_b.blockchain = 'avalanche_c'
-- price of sold tokens
left join {{ source('prices', 'usd') }} prices_s
    on prices_s.minute = date_trunc('minute', s.evt_block_time)
    and prices_s.contract_address = s.fromToken
    and prices_s.blockchain = 'avalanche_c'
where 1 = 1
    {% if is_incremental() %}
    and s.evt_block_time >= date_trunc("day", now() - interval '1 week')
    and prices_b.minute >= date_trunc("day", now() - interval '1 week')
    and prices_s.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
	{% if not is_incremental() %}
    and prices_b.minute >= '{{project_start_date}}'
    and prices_s.minute >= '{{project_start_date}}'
    {% endif %}
