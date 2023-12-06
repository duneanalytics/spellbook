{{  
    config(
        schema = 'oneinch',
        alias = 'lop_own_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index']
    )
}}



with
    
    orders as (
        select
            blockchain
            , block_time
            , date(block_time) as block_date
            , block_month
            , minute
            , tx_hash
            , tx_from
            , tx_to
            , call_to as project_contract_address
            , if(lower(method) like '%rfq%', protocol_version||' RFQ', protocol_version) as version
            , call_trace_address
            , maker
            , maker_asset as src_token_address
            , making_amount as src_amount
            , call_from as taker
            , taker_asset as dst_token_address
            , taking_amount as dst_amount
            , '1inch LOP' as project
        from (
            select * from {{ ref('oneinch_lop') }}
            {% if is_incremental() %}
                where {{ incremental_predicate('block_time') }}
            {% endif %}
        )
        left join (
            select blockchain, contract_address as call_from, true as fusion
            from {{ ref('oneinch_fusion_settlements') }}
        ) using(blockchain, call_from)
        where tx_success and call_success and fusion is null -- exclude orders that were called by Fusion settlement contracts
        
    )

    , prices_src as (
        select
            blockchain
            , contract_address as src_token_address
            , minute
            , price as src_price
        from {{ source('prices', 'usd') }}
        {% if is_incremental() %}
            where {{ incremental_predicate('minute') }}
        {% endif %}
    )

    , prices_dst as (
        select
            blockchain
            , contract_address as dst_token_address
            , minute
            , price as dst_price
        from {{ source('prices', 'usd') }}
        {% if is_incremental() %}
            where {{ incremental_predicate('minute') }}
        {% endif %}
    )

    , tokens_src as (
        select 
            blockchain
            , contract_address as src_token_address
            , symbol
            , decimals
        from {{ ref('tokens_erc20') }}
    )

    , tokens_dst as (
        select 
            blockchain
            , contract_address as dst_token_address
            , symbol
            , decimals
        from {{ ref('tokens_erc20') }}
    )

    , additions as (
        select
            blockchain
            , project
            , version
            , block_date
            , block_month
            , block_time
            , coalesce(tokens_dst.symbol, '') as token_bought_symbol
            , coalesce(tokens_src.symbol, '') as token_sold_symbol
            , array_join(array_sort(array[coalesce(tokens_src.symbol, ''), coalesce(tokens_dst.symbol, '')]), '-') as token_pair
            , cast(dst_amount as double) / pow(10, tokens_dst.decimals) as token_bought_amount
            , cast(src_amount as double) / pow(10, tokens_src.decimals) as token_sold_amount
            , dst_amount as token_bought_amount_raw
            , src_amount as token_sold_amount_raw
            , coalesce(
                cast(src_amount as double) / pow(10, tokens_src.decimals) * src_price, 
                cast(dst_amount as double) / pow(10, tokens_dst.decimals) * dst_price
            ) as amount_usd
            , dst_token_address as token_bought_address
            , src_token_address as token_sold_address
            , taker
            , maker
            , project_contract_address
            , tx_hash
            , tx_from
            , tx_to
            , row_number() over(partition by tx_hash order by call_trace_address) as evt_index
        from orders
        left join prices_src using(blockchain, src_token_address, minute)
        left join prices_dst using(blockchain, dst_token_address, minute)
        left join tokens_src using(blockchain, src_token_address)
        left join tokens_dst using(blockchain, dst_token_address)
    )

select *
from additions