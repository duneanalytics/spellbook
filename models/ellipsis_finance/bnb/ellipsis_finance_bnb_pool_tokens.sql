{{ config(
    tags=['dunesql'],
    alias = alias('pool_tokens'),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'token_id', 'token_type']
    )
}}
 
WITH 

base_pools as ( -- this gets the base pools deployed on ellipsis
        SELECT 
            _base_pool as pool, 
            _lp_token as lp_token 
        FROM 
        {{ source('ellipsis_finance_bnb', 'FactoryPool_call_add_base_pool') }}
        WHERE call_success = true 
        {% if is_incremental() %}
        AND call_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        
        UNION ALL
        
        SELECT 
            _base_pool as pool, 
            _lp_token as lp_token
        FROM 
        {{ source('ellipsis_finance_bnb', 'FactoryPool_v2_call_add_base_pool') }}
        WHERE call_success = true 
        {% if is_incremental() %}
        AND call_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL -- manually importanting base pools that weren't created by the factory (no events emitted for them)

        SELECT 
            pool, 
            lp_token 
        FROM (
        VALUES 
        -- valtusd_val3eps
            (0xAB499095961516f058245C1395f9c0410764b6Cd, 0xF6be0F52Be5e68DF4Ed3ea7cCD569C16024C250D),
        -- valdai_val3eps
            (0x245e8bb5427822FB8fd6cE062d8dd853FbcfABF5, 0x8087a94FFE6bcF08DC4b4EBB3d28B4Ed75a792aC)
        ) as temp_table (pool, lp_token)
), 

hardcoded_underlying as ( -- harcoding the underlying tokens here as there's no event emitted that gives a list of the tokens 
        SELECT 
            pool, 
            token_id, 
            token_address
        FROM (
        VALUES 
        -- val3EPS 
            (0x5b5bD8913D766D005859CE002533D4838B0Ebbb5, 0, 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
            (0x5b5bD8913D766D005859CE002533D4838B0Ebbb5, 1, 0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d),
            (0x5b5bD8913D766D005859CE002533D4838B0Ebbb5, 2, 0x55d398326f99059fF775485246999027B3197955), 
        -- 3eps 
            (0xaF4dE8E872131AE328Ce21D909C74705d3Aaf452, 0, 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
            (0xaF4dE8E872131AE328Ce21D909C74705d3Aaf452, 1, 0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d),
            (0xaF4dE8E872131AE328Ce21D909C74705d3Aaf452, 2, 0x55d398326f99059fF775485246999027B3197955), 
        -- valbtc_renbtc
            (0xdc7f3e34c43f8700b0eb58890add03aa84f7b0e1, 0, 0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c), 
            (0xdc7f3e34c43f8700b0eb58890add03aa84f7b0e1, 1, 0xfCe146bF3146100cfe5dB4129cf6C82b0eF4Ad8c),
        -- valtusd_val3eps
            (0xF6be0F52Be5e68DF4Ed3ea7cCD569C16024C250D, 0, 0x14016E85a25aeb13065688cAFB43044C2ef86784), 
            (0xF6be0F52Be5e68DF4Ed3ea7cCD569C16024C250D, 1, 0xaeD19DAB3cd68E4267aec7B2479b1eD2144Ad77f),
            (0xF6be0F52Be5e68DF4Ed3ea7cCD569C16024C250D, 2, 0xA6fDEa1655910C504E974f7F1B520B74be21857B), 
            (0xF6be0F52Be5e68DF4Ed3ea7cCD569C16024C250D, 3, 0x5f7f6cB266737B89f7aF86b30F03Ae94334b83e9), 
        -- valdai_val3eps
            (0x8087a94FFE6bcF08DC4b4EBB3d28B4Ed75a792aC, 0, 0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3), 
            (0x8087a94FFE6bcF08DC4b4EBB3d28B4Ed75a792aC, 1, 0xaeD19DAB3cd68E4267aec7B2479b1eD2144Ad77f),
            (0x8087a94FFE6bcF08DC4b4EBB3d28B4Ed75a792aC, 2, 0xA6fDEa1655910C504E974f7F1B520B74be21857B), 
            (0x8087a94FFE6bcF08DC4b4EBB3d28B4Ed75a792aC, 3, 0x5f7f6cB266737B89f7aF86b30F03Ae94334b83e9)
        ) as temp_table (pool, token_id, token_address)
), 

base_pools_underlying_tokens_bought as ( -- because when you trade on ellipsis (the underlying exchange event) you can only buy the underlying tokens weirdly, you can't sell them or rather if you do, the contarct actually only sells the lp token so we need underlying boought & sold tables
        SELECT 
            b.pool, 
            b.lp_token, 
            a.token_id, 
            a.token_address, 
            'underlying_token_bought' as token_type, 
            'Base Pool' as pool_type
        FROM 
        hardcoded_underlying a 
        INNER JOIN 
        base_pools b 
            ON a.pool = b.lp_token -- joining here to get the pool details 
), 

base_pools_pool_tokens as ( -- these are the pool coins (different from underlying for val3ps & valbtc-renbtc because they're pools of pool tokens)
        SELECT 
            pool, 
            token_id, 
            token_address, 
            'pool_token' as token_type, 
            'Base Pool' as pool_type
        FROM (
        VALUES 
        -- 3eps 
            (0x160CAed03795365F3A589f10C379FfA7d75d4E76, 0, 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
            (0x160CAed03795365F3A589f10C379FfA7d75d4E76, 1, 0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d),
            (0x160CAed03795365F3A589f10C379FfA7d75d4E76, 2, 0x55d398326f99059fF775485246999027B3197955),
        -- val3ps 
            (0x19ec9e3f7b21dd27598e7ad5aae7dc0db00a806d, 0, 0xaeD19DAB3cd68E4267aec7B2479b1eD2144Ad77f),
            (0x19ec9e3f7b21dd27598e7ad5aae7dc0db00a806d, 1, 0xA6fDEa1655910C504E974f7F1B520B74be21857B), 
            (0x19ec9e3f7b21dd27598e7ad5aae7dc0db00a806d, 2, 0x5f7f6cB266737B89f7aF86b30F03Ae94334b83e9), 
        -- valbtc_renbtc 
            (0xfa715e7c8fa704cf425dd7769f4a77b81420fbf2, 0, 0x204992f7fCBC4c0455d7Fec5f712BeDd98E7d6d6), 
            (0xfa715e7c8fa704cf425dd7769f4a77b81420fbf2, 1, 0xfCe146bF3146100cfe5dB4129cf6C82b0eF4Ad8c),
        -- valtusd_val3ps
            (0xAB499095961516f058245C1395f9c0410764b6Cd, 0, 0xBB5DDE96BAD874e4FFe000B41Fa5E98F0665a4BC), 
            (0xAB499095961516f058245C1395f9c0410764b6Cd, 1, 0x5b5bD8913D766D005859CE002533D4838B0Ebbb5), -- duplicate for both pools because there's token id 2 & 3 in exchange underlying 
            (0xAB499095961516f058245C1395f9c0410764b6Cd, 2, 0x5b5bD8913D766D005859CE002533D4838B0Ebbb5),
            (0xAB499095961516f058245C1395f9c0410764b6Cd, 3, 0x5b5bD8913D766D005859CE002533D4838B0Ebbb5), 
        -- valdai_val3ps
            (0x245e8bb5427822FB8fd6cE062d8dd853FbcfABF5, 0, 0x2c85EBAE81b7078Cd656b2C6e2d58411cB41D91A),
            (0x245e8bb5427822FB8fd6cE062d8dd853FbcfABF5, 1, 0x5b5bD8913D766D005859CE002533D4838B0Ebbb5),
            (0x245e8bb5427822FB8fd6cE062d8dd853FbcfABF5, 2, 0x5b5bD8913D766D005859CE002533D4838B0Ebbb5),
            (0x245e8bb5427822FB8fd6cE062d8dd853FbcfABF5, 3, 0x5b5bD8913D766D005859CE002533D4838B0Ebbb5)
        ) as temp_table (pool, token_id, token_address)
), 

base_pools_underlying_tokens_sold as ( -- the underlying tokens sold works different for base pools so we're using duplicate the pool tokens here
        SELECT 
            pool, 
            token_id, 
            token_address, 
            'underlying_token_sold' as token_type, 
            pool_type
        FROM 
        base_pools_pool_tokens
), 

plain_pools as ( -- getting plain pools data 
        SELECT 
            pool, 
            token_id,
            token_address,
            token_type
        FROM 
        (
            SELECT
                pool,
                'pool_token' as token_type,
                (token_id - 1) as token_id,
                token_address
            FROM
            {{ source('ellipsis_finance_bnb', 'FactoryPool_evt_PlainPoolDeployed') }}
            CROSS JOIN UNNEST (coins)
            WITH ORDINALITY AS _u (token_address, token_id)
            {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}

            UNION ALL

            SELECT
                pool,
                'pool_token' as token_type,
                (token_id - 1) as token_id,
                token_address
            FROM
            {{ source('ellipsis_finance_bnb', 'FactoryPool_v2_evt_PlainPoolDeployed') }}
            CROSS JOIN UNNEST (coins)
            WITH ORDINALITY AS _u (token_address, token_id)
            {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        ) x 
), 

plain_pools_pool_tokens as (
        SELECT 
            *, 
            'Plain Pool' as pool_type 
        FROM 
        plain_pools
        WHERE token_address <> 0x0000000000000000000000000000000000000000 -- the 3rd & 4th coins are usually 0x000 so filtering here
), 

meta_pools as ( -- getting meta pools and their base pools 
        SELECT 
            pool, 
            token_id,
            token_address,
            base_pool
        FROM 
        (
            SELECT
                pool,
                base_pool,
                (token_id - 1) as token_id,
                token_address
            FROM
            {{ source('ellipsis_finance_bnb', 'FactoryPool_evt_MetaPoolDeployed') }}
            CROSS JOIN UNNEST (coins)
            WITH ORDINALITY AS _u (token_address, token_id)
            {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}

            UNION ALL

            SELECT
                pool,
                base_pool,
                (token_id - 1) as token_id,
                token_address
            FROM
            {{ source('ellipsis_finance_bnb', 'FactoryPool_v2_evt_MetaPoolDeployed') }}
            CROSS JOIN UNNEST (coins)
            WITH ORDINALITY AS _u (token_address, token_id)
            {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        ) x 
), 

meta_pools_pool_tokens as ( -- no filter needed for the pool tokens
        SELECT 
            pool, 
            token_id, 
            token_address, 
            'pool_token' as token_type, 
            'Meta Pool' as pool_type
        FROM 
        meta_pools
), 

meta_pool_distinct as ( -- getting the distinct pool & their base pools 
        SELECT DISTINCT 
            pool, 
            base_pool
        FROM 
        meta_pools
), 

meta_pools_underlying_tokens_bought as ( -- getting the underlying token bought, using the base pool pools token table from earlier
        SELECT 
            pool, 
            token_address, 
            token_id,
            'underlying_token_bought' as token_type,
            'Meta Pool' as pool_type
        FROM 
        meta_pools_pool_tokens
        WHERE token_id = 0
        
        UNION ALL
        
        SELECT 
            mt.pool, 
            bt.token_address, 
            CASE -- reordering here as the '0' token of the metapool is first and then the pool tokens
                WHEN bt.token_id = 0 THEN 1
                WHEN bt.token_id = 1 THEN 2
                WHEN bt.token_id = 2 THEN 3
            END as token_id, 
            'underlying_token_bought' as token_type, 
            'Meta Pool' as pool_type
        FROM 
        meta_pool_distinct mt 
        INNER JOIN 
        base_pools_pool_tokens bt 
            ON mt.base_pool = bt.pool
), 

meta_pools_underlying_tokens_sold as ( -- getting the underlying tokens sold for meta pools 
        SELECT 
            pool, 
            token_address, 
            token_id,
            'underlying_token_sold' as token_type,
            'Meta Pool' as pool_type
        FROM 
        meta_pools_pool_tokens
        WHERE token_id = 0
        
        UNION ALL  -- duplicate here because the pool token (1) is also token 2 & 3

        SELECT 
            pool, 
            token_address, 
            token_id, 
            'underlying_token_sold' as token_type,
            'Meta Pool' as pool_type
        FROM 
        meta_pools_pool_tokens
        WHERE token_id = 1
        
        UNION ALL

        SELECT 
            pool, 
            token_address, 
            2 as  token_id, 
            'underlying_token_sold' as token_type,
            'Meta Pool' as pool_type
        FROM 
        meta_pools_pool_tokens
        WHERE token_id = 1
        
        UNION ALL
        
        SELECT 
            pool, 
            token_address, 
            3 as  token_id, 
            'underlying_token_sold' as token_type,
            'Meta Pool' as pool_type
        FROM 
        meta_pools_pool_tokens
        WHERE token_id = 1
), 

crypto_pools as ( -- getting crypto pools 
        SELECT 
            pool, 
            token_id,
            token_address,
            token_type
        FROM 
        (
            SELECT
                pool,
                'pool_token' as token_type,
                (token_id - 1) as token_id,
                token_address
            FROM
            {{ source('ellipsis_finance_bnb', 'FactoryPool_v3_evt_CryptoPoolDeployed') }}
            CROSS JOIN UNNEST (coins)
            WITH ORDINALITY AS _u (token_address, token_id)
            {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}

            UNION ALL

            SELECT
                pool,
                'pool_token' as token_type,
                (token_id - 1) as token_id,
                token_address
            FROM
            {{ source('ellipsis_finance_bnb', 'FactoryPool_v4_evt_CryptoPoolDeployed') }}
            CROSS JOIN UNNEST (coins)
            WITH ORDINALITY AS _u (token_address, token_id)
            {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        ) x 
), 

crypto_pools_pool_tokens as (
        SELECT 
            *, 
            'Crypto Pool' as pool_type 
        FROM 
        crypto_pools
), 

all_pool_tokens as (
        SELECT pool, token_id, token_address, token_type, pool_type FROM base_pools_pool_tokens
        UNION 
        SELECT pool, token_id, token_address, token_type, pool_type FROM base_pools_underlying_tokens_bought
        UNION 
        SELECT pool, token_id, token_address, token_type, pool_type FROM base_pools_underlying_tokens_sold
        UNION 
        SELECT pool, token_id, token_address, token_type, pool_type FROM plain_pools_pool_tokens
        UNION 
        SELECT pool, token_id, token_address, token_type, pool_type FROM meta_pools_pool_tokens
        UNION 
        SELECT pool, token_id, token_address, token_type, pool_type FROM meta_pools_underlying_tokens_bought
        UNION 
        SELECT pool, token_id, token_address, token_type, pool_type FROM meta_pools_underlying_tokens_sold
        UNION 
        SELECT pool, token_id, token_address, token_type, pool_type FROM crypto_pools_pool_tokens
)

SELECT 
    'bnb' as blockchain, 
    'ellipsis_finance' as project, 
    '1' as version, 
    pool, 
    token_id,
    token_address, 
    token_type, 
    pool_type
FROM 
all_pool_tokens

