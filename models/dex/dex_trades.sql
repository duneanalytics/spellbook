{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum"]\',
                                "sector",
                                "dex",
                                \'["jeff-dude", "hosuke", "0xRob", "pandajackson42", "Henrystats", "scoffie", "zhongyiio", "justabi", "umer_h_adil", "mtitus6", "dbustos20", "tian7", "bh2smith"]\') }}'
        )
}}

/*
list of models using old generic test, due to multiple versions in one model:
    - curvefi_trades
    - airswap_ethereum_trades
    - dodo_ethereum_trades
    - bancor_ethereum_trades
    - mstable_ethereum_trades
*/

{% set dex_trade_models = [
 ref('uniswap_trades')
,ref('sushiswap_trades')
,ref('kyberswap_trades')
,ref('fraxswap_trades')
,ref('curvefi_trades')
,ref('airswap_ethereum_trades')
,ref('clipper_ethereum_trades')
,ref('shibaswap_ethereum_trades')
,ref('swapr_ethereum_trades')
,ref('defiswap_ethereum_trades')
,ref('dfx_ethereum_trades')
,ref('pancakeswap_trades')
,ref('dodo_trades')
,ref('velodrome_optimism_trades')
,ref('woofi_trades')
,ref('bancor_ethereum_trades')
,ref('platypus_finance_avalanche_c_trades')
,ref('trader_joe_trades')
,ref('hashflow_trades')
,ref('mstable_ethereum_trades')
,ref('mdex_bnb_trades')
,ref('zigzag_trades')
,ref('nomiswap_bnb_trades')
,ref('gmx_trades')
,ref('biswap_bnb_trades') 
,ref('wombat_bnb_trades')
,ref('iziswap_bnb_trades')
,ref('babyswap_bnb_trades')
,ref('ellipsis_finance_trades')
] %}


SELECT *
FROM (
    {% for dex_model in dex_trade_models %}
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
        CAST(token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw,
        CAST(token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw,
        amount_usd,
        token_bought_address,
        token_sold_address,
        taker,
        maker,
        project_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        trace_address,
        evt_index
    FROM {{ dex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
