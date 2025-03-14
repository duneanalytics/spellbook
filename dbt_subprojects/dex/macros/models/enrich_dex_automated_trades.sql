{% macro enrich_dex_automated_trades(
    base_trades = null
    , tokens_erc20_model = source('tokens', 'erc20')
    , project = False
)
%}

with enrichments AS (
    select
        base_trades.blockchain
        , base_trades.version
        , base_trades.dex_type
        {%- if project %}
        , project
        {%- endif %}
        , base_trades.block_month
        , base_trades.block_date
        , base_trades.block_time
        , base_trades.block_number
        , erc20_bought.symbol AS token_bought_symbol
        , erc20_sold.symbol AS token_sold_symbol
        , case
            when lower(erc20_bought.symbol) > lower(erc20_sold.symbol) then concat(erc20_sold.symbol, '-', erc20_bought.symbol)
            else concat(erc20_bought.symbol, '-', erc20_sold.symbol)
            end AS token_pair
        , base_trades.token_bought_amount_raw / power(10, erc20_bought.decimals) AS token_bought_amount
        , base_trades.token_sold_amount_raw / power(10, erc20_sold.decimals) AS token_sold_amount
        , base_trades.token_bought_amount_raw
        , base_trades.token_sold_amount_raw
        , base_trades.token_bought_address
        , base_trades.token_sold_address
        , coalesce(base_trades.taker, base_trades.tx_from) AS taker
        , base_trades.maker
        , base_trades.project_contract_address
        , base_trades.pool_topic0
        , base_trades.factory_address
        , base_trades.factory_topic0
        , base_trades.factory_info
        , base_trades.tx_hash
        , base_trades.tx_from
        , base_trades.tx_to
        , base_trades.evt_index
        , base_trades.tx_index
    from {{ base_trades }} as base_trades
    left join {{ tokens_erc20_model }} as erc20_bought
        on erc20_bought.contract_address = base_trades.token_bought_address
        and erc20_bought.blockchain = base_trades.blockchain
    left join {{ tokens_erc20_model }} as erc20_sold
        on erc20_sold.contract_address = base_trades.token_sold_address
        and erc20_sold.blockchain = base_trades.blockchain
    {% if is_incremental() %}
    where 
        {{ incremental_predicate('base_trades.block_time') }}
    {% endif %}
)

, enrichments_with_prices AS (
    {{
        add_amount_usd(
            trades_cte = 'enrichments'
        )
    }}
)

select
    blockchain
    , version
    , dex_type
    {%- if project %}
    , project
    {%- endif %}
    , block_month
    , block_date
    , block_time
    , block_number
    , token_bought_symbol
    , token_sold_symbol
    , token_pair
    , token_bought_amount
    , token_sold_amount
    , token_bought_amount_raw
    , token_sold_amount_raw
    , amount_usd
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , pool_topic0
    , factory_address
    , factory_topic0
    , factory_info
    , tx_hash
    , tx_from
    , tx_to
    , evt_index
    , tx_index
from enrichments_with_prices

{% endmacro %}