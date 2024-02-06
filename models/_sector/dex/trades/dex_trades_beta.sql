{{ config(
    schema = 'dex'
    , alias = 'trades_beta'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set as_is_models = [
    ref('oneinch_lop_own_trades')
    , ref('zeroex_native_trades')

] %}

WITH curve AS (
    {{
        enrich_curve_dex_trades(
            base_trades = ref('dex_base_trades')
            , filter = "project = 'curve'"
            , curve_ethereum = ref('curvefi_ethereum_base_trades')
            , curve_optimism = ref('curvefi_optimism_base_trades')
            , tokens_erc20_model = source('tokens', 'erc20')
            , prices_model = source('prices', 'usd')
        )
    }}
)
, dexs AS (
    {{
        enrich_dex_trades(
            base_trades = ref('dex_base_trades')
            , filter = "project != 'curve'"
            , tokens_erc20_model = source('tokens', 'erc20')
            , prices_model = source('prices', 'usd')
        )
    }}
)
, as_is_dexs AS (
    {% for model in as_is_models %}
    SELECT
        *
        , NULL as block_number  -- we may solve this in the future
    FROM
        {{ model }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('block_time') }}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

SELECT
    *
FROM
    curve
UNION ALL
SELECT
    *
FROM
    as_is_dexs
UNION ALL
SELECT
    *
FROM
    dexs