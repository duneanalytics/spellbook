{{ config(
	tags=['legacy'],
	
        alias = alias('trades', legacy_model=True),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon"]\',
                                "sector",
                                "dex",
                                \'["jeff-dude", "hosuke", "0xRob", "pandajackson42", "Henrystats", "scoffie", "zhongyiio", "justabi", "umer_h_adil", "mtitus6", "dbustos20", "tian7", "bh2smith", "rantum", "mike-x7f"]\') }}'
        )
}}

 
{% set dex_trade_models = [
 ref('uniswap_trades_legacy')
,ref('sushiswap_trades_legacy')
,ref('kyberswap_trades_legacy')
,ref('fraxswap_trades_legacy')
,ref('curvefi_trades_legacy')
,ref('airswap_ethereum_trades_legacy')
,ref('clipper_trades_legacy')
,ref('shibaswap_ethereum_trades_legacy')
,ref('swapr_ethereum_trades_legacy')
,ref('defiswap_ethereum_trades_legacy')
,ref('dfx_ethereum_trades_legacy')
,ref('pancakeswap_trades_legacy')
,ref('dodo_trades_legacy')
,ref('velodrome_optimism_trades_legacy')
,ref('woofi_trades_legacy')
,ref('bancor_ethereum_trades_legacy')
,ref('platypus_finance_avalanche_c_trades_legacy')
,ref('trader_joe_trades_legacy')
,ref('hashflow_trades_legacy')
,ref('mstable_ethereum_trades_legacy')
,ref('mdex_bnb_trades_legacy')
,ref('zigzag_trades_legacy')
,ref('nomiswap_bnb_trades_legacy')
,ref('gmx_trades_legacy')
,ref('biswap_bnb_trades_legacy')
,ref('wombat_bnb_trades_legacy')
,ref('iziswap_bnb_trades_legacy')
,ref('babyswap_bnb_trades_legacy')
,ref('apeswap_trades_legacy')
,ref('ellipsis_finance_trades_legacy')
,ref('spartacus_exchange_fantom_trades_legacy')
,ref('spookyswap_fantom_trades_legacy')
,ref('beethoven_x_trades_legacy')
,ref('rubicon_trades_legacy')
,ref('synthetix_spot_trades_legacy')
,ref('zipswap_trades_legacy')
,ref('equalizer_exchange_fantom_trades_legacy')
,ref('wigoswap_fantom_trades_legacy')
,ref('arbswap_trades_legacy')
,ref('balancer_trades_legacy')
,ref('spiritswap_fantom_trades_legacy')
,ref('quickswap_trades_legacy')
,ref('integral_trades_legacy')
,ref('maverick_trades_legacy')
,ref('verse_dex_ethereum_trades_legacy')
,ref('onepunchswap_bnb_trades_legacy')
,ref('glacier_avalanche_c_trades_legacy')
,ref('thena_trades_legacy')
,ref('camelot_trades_legacy')
,ref('zeroex_native_trades_legacy')
,ref('xchange_trades_legacy')
] %}


SELECT  blockchain,
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
        CAST(token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw, --remove in dunesql migration, we should cast to UINT256 in all upsteam level spells
        CAST(token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw, --remove in dunesql migration, we should cast to UINT256 in all upsteam level spells
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
        evt_index,
        row_number() over (partition by tx_hash, evt_index, trace_address order by tx_hash) as duplicates_rank
    FROM {{ dex_model }}
    {% if not loop.last %}
    {% if is_incremental() %}
    WHERE block_date >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
WHERE duplicates_rank = 1
