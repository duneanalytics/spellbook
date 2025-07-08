{{config(
    schema = 'tokens_ton',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'tx_hash', 'tx_lt', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ expose_spells(\'["ton"]\',
                                "sector",
                                "tokens",
                                \'["krishhh"]\') }}'
)
}}

WITH ton_prices AS (
    SELECT
        date_trunc('day', minute) as block_date,
        avg(price) as price,
        symbol,
        9 as token_decimals
    FROM {{ source('prices', 'usd') }}
    WHERE symbol = 'TON' 
        AND blockchain is null
    {% if is_incremental() %}
        AND {{ incremental_predicate('minute') }}
    {% endif %}
    GROUP BY 1
),

jetton_prices AS (
    SELECT 
        jp.token_address as jetton_master,
        CASE
            WHEN jp.token_address = '0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE' -- USDT
                OR jp.token_address = '0:BDF3FA8098D129B54B4F73B5BAC5D1E1FD91EB054169C3916DFC8CCD536D1000' -- tsTON
                OR jp.token_address = '0:CD872FA7C5816052ACDF5332260443FAEC9AACC8C21CCA4D92E7F47034D11892' -- stTON
                OR jp.token_address = '0:CF76AF318C0872B58A9F1925FC29C156211782B9FB01F56760D292E56123BF87' -- hTON
            THEN 0 -- USDT and LSDs are liquid and don't need liquidity limits
            WHEN asset_type = 'Jetton' THEN 1 -- other jettons need DEX liquidity check
            ELSE 0 -- DEX LPs and SLPs liquidity guaranteed by smart contracts
        END as is_need_liquidity_limit,
        jp.timestamp as block_date,
        jp.price_usd,
        jp.asset_type
    FROM {{ ref('ton_jetton_price_daily') }} jp
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('jp.timestamp') }}
    {% endif %}
),

daily_liquidity AS (
    SELECT 
        block_date, 
        jetton_master, 
        sum(tvl_usd) as total_token_tvl_usd 
    FROM (
        SELECT 
            block_date, 
            pool, 
            jetton_left, 
            jetton_right, 
            avg(tvl_usd) as tvl_usd 
        FROM {{ source('ton', 'dex_pools') }}
        WHERE tvl_usd > 0
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_date') }}
        {% endif %}
        GROUP BY 1, 2, 3, 4
    ) sub
    CROSS JOIN unnest(array[jetton_left, jetton_right]) as t(jetton_master)
    GROUP BY 1, 2
),

-- Native TON transfers
native_ton_transfers AS (
    SELECT
        'ton' as blockchain,
        date_trunc('month', tm.block_date) as block_month,
        tm.block_date,
        tm.block_time,
        tm.tx_hash,
        CAST(null AS bigint) as tx_index,
        0 as evt_index,
        tm.tx_lt,
        'native' as transfer_type,
        'native' as token_standard,
        'transfer' as transaction_type,
        'success' as transaction_result,
        CAST(null AS varchar) as tx_from,
        CAST(null AS varchar) as tx_to,
        tm.source as from_address,
        tm.destination as to_address,
        CAST(null AS varchar) as contract_address,
        CAST(null AS varchar) as token_address,
        tp.symbol,
        tp.token_decimals,    
        tm.value as amount_raw,
        tm.value / power(10, tp.token_decimals) as amount,
        (tm.value / power(10, tp.token_decimals)) * tp.price as amount_usd,
        tp.price as price_usd
    FROM {{ source('ton', 'messages') }} tm
    JOIN ton_prices tp ON tm.block_date = tp.block_date
    WHERE tm.direction = 'in'
        AND tm.value > 0
        {% if is_incremental() %}
        AND {{ incremental_predicate('tm.block_date') }}
        {% endif %}
),

-- Jetton transfers with proper metadata
jetton_transfers AS (
    SELECT
        'ton' as blockchain,
        date_trunc('month', je.block_date) as block_month,
        je.block_date,
        je.block_time,
        je.tx_hash,
        CAST(null AS bigint) as tx_index,
        0 as evt_index,
        je.tx_lt,
        'jetton' as transfer_type,
        COALESCE(jp.asset_type, 'jetton') as token_standard,
        'transfer' as transaction_type,
        CASE WHEN NOT je.tx_aborted THEN 'success' ELSE 'failed' END as transaction_result,
        CAST(null AS varchar) as tx_from,
        CAST(null AS varchar) as tx_to,
        je.source as from_address,
        je.destination as to_address,
        je.jetton_wallet as contract_address,
        je.jetton_master as token_address,
        COALESCE(jm.symbol, 'UNKNOWN') as symbol,
        COALESCE(jm.decimals, 9) as token_decimals,
        je.amount as amount_raw,
        je.amount / power(10, COALESCE(jm.decimals, 9)) as amount,  
        CASE
            WHEN jp.is_need_liquidity_limit = 0 
                THEN (je.amount / power(10, COALESCE(jm.decimals, 9))) * COALESCE(jp.price_usd, 0)
            WHEN jp.is_need_liquidity_limit = 1
                THEN least((je.amount / power(10, COALESCE(jm.decimals, 9))) * COALESCE(jp.price_usd, 0), coalesce(dl.total_token_tvl_usd, 0))
            ELSE 0 
        END as amount_usd,
        jp.price_usd
    FROM {{ source('ton', 'jetton_events') }} je
    LEFT JOIN jetton_prices jp ON je.jetton_master = jp.jetton_master AND je.block_date = jp.block_date
    LEFT JOIN daily_liquidity dl ON je.block_date = dl.block_date AND je.jetton_master = dl.jetton_master
    LEFT JOIN {{ ref('ton_latest_jetton_metadata') }} jm ON je.jetton_master = jm.address
    WHERE je.type = 'transfer'
        AND je.jetton_master != upper('0:8cdc1d7640ad5ee326527fc1ad0514f468b30dc84b0173f0e155f451b4e11f7c') -- pTON
        AND je.jetton_master != upper('0:671963027f7f85659ab55b821671688601cdcf1ee674fc7fbbb1a776a18d34a3') -- pTON
        AND NOT je.tx_aborted
        AND je.amount > 0
        {% if is_incremental() %}
        AND {{ incremental_predicate('je.block_date') }}
        {% endif %}
)

SELECT 
    *
FROM native_ton_transfers

UNION ALL

SELECT 
    *
FROM jetton_transfers
