{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum"]\',
                                "sector",
                                "dex",
                                \'["jeff-dude", "hosuke", "0xRob", "pandajackson42", "Henrystats", "scoffie", "zhongyiio", "justabi", "umer_h_adil", "mtitus6", "dbustos20", "tian7"]\') }}'
        )
}}

{% set dex_trade_models = [
'uniswap_trades'
,'sushiswap_trades'
,'kyberswap_trades'
,'fraxswap_trades'
,'curvefi_trades'
,'airswap_ethereum_trades'
,'clipper_ethereum_trades'
,'shibaswap_ethereum_trades'
,'swapr_ethereum_trades'
,'defiswap_ethereum_trades'
,'dfx_ethereum_trades'
,'pancakeswap_trades'
,'dodo_ethereum_trades'
,'woofi_avalanche_c_trades'
,'bancor_ethereum_trades'
,'platypus_finance_avalanche_c_trades'
,'hashflow_trades'
,'mstable_ethereum_trades'
,'zigzag_trades'
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
        token_bought_amount_raw,
        token_sold_amount_raw,
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
    FROM {{ ref(dex_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
