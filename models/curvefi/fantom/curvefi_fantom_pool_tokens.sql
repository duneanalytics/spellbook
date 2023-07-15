{{ config(
    alias = alias('pool_tokens'),
    partition_by = ['pool'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'token_id', 'token_type']
    )
}}
 
WITH 

base_pools as ( -- this gets the base pools deployed on curvefi 
        SELECT 
            base_pool as pool
        FROM 
        {{ source('curvefi_fantom', 'StableSwap_Factory_evt_BasePoolAdded') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

base_pools_lp_tokens as ( -- the lp tokens aren't in the call or events for meta pools and we need them to get the token id (1) of meta pools 
        SELECT 
            LOWER(pool) as pool, 
            LOWER(lp_token) as lp_token 
        FROM (
        VALUES 
        -- renbtc pool 
            ('0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604', '0x5B5CFE992AdAC0C9D48E05854B2d91C73a003858'),
        -- geist pool 
            ('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '0xD02a30d33153877BC20e5721ee53DeDEE0422B2F'),
        -- 2pool dai + usdc 
            ('0x27e611fd27b276acbd5ffd632e5eaebec9761e40', '0x27E611FD27b276ACbd5Ffd632E5eAEBEC9761E40') -- lp token same as pool address 
        ) as temp_table (pool, lp_token)
), 

hardcoded_underlying as ( -- harcoding the underlying tokens here as there's no event emitted that gives a list of the tokens 
        SELECT 
            LOWER(pool) as pool, 
            token_id, 
            LOWER(token_address) as token_address
        FROM (
        VALUES 
        -- renBTC pool
            ('0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604', '0', '0x321162Cd933E2Be498Cd2267a90534A804051b11'), -- wbtc 
            ('0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604', '1', '0xDBf31dF14B66535aF65AaC99C32e9eA844e14501'), -- renbtc
        -- geist pool
            ('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '0', '0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E'), -- dai
            ('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '1', '0x04068DA6C83AFCFA0e13ba15A6696662335D5B75'), -- usdc 
            ('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '2', '0x049d68029688eAbF473097a2fC38ef61633A3C7A'), -- fusdt
        -- 2pool dai + usdc 
            ('0x27e611fd27b276acbd5ffd632e5eaebec9761e40', '0', '0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E'), -- dai 
            ('0x27e611fd27b276acbd5ffd632e5eaebec9761e40', '1', '0x04068DA6C83AFCFA0e13ba15A6696662335D5B75') -- usdc 
        ) as temp_table (pool, token_id, token_address)
), 

base_pools_underlying_tokens_bought as ( -- because when you trade on ellipsis (the underlying exchange event) you can only buy the underlying tokens weirdly, you can't sell them or rather if you do, the contarct actually only sells the lp token so we need underlying boought & sold tables
        SELECT 
            b.pool, 
            a.token_id, 
            a.token_address, 
            'underlying_token_bought' as token_type, 
            'Base Pool' as pool_type
        FROM 
        hardcoded_underlying a 
        INNER JOIN 
        base_pools b 
            ON a.pool = b.pool -- joining here to get the pool details 
), 

base_pools_pool_tokens as ( -- these are the pool coins (different from underlying for geist pool because g tokens are some sort of wrapped version of the underlying tokens)
        SELECT 
            LOWER(pool) as pool, 
            token_id, 
            LOWER(token_address) as token_address, 
            'pool_token' as token_type, 
            'Base Pool' as pool_type
        FROM (
        VALUES 
        -- renBTC pool
            ('0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604', '0', '0x321162Cd933E2Be498Cd2267a90534A804051b11'), -- wbtc 
            ('0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604', '1', '0xDBf31dF14B66535aF65AaC99C32e9eA844e14501'), -- renbtc
        -- geist pool
            ('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '0', '0x07E6332dD090D287d3489245038daF987955DCFB'), -- gdai
            ('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '1', '0xe578C856933D8e1082740bf7661e379Aa2A30b26'), -- gusdc 
            ('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '2', '0x940F41F0ec9ba1A34CF001cc03347ac092F5F6B5'), -- gfusdt
        -- 2pool dai + usdc 
            ('0x27e611fd27b276acbd5ffd632e5eaebec9761e40', '0', '0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E'), -- dai 
            ('0x27e611fd27b276acbd5ffd632e5eaebec9761e40', '1', '0x04068DA6C83AFCFA0e13ba15A6696662335D5B75') -- usdc 
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
            CAST(token_id AS STRING) as token_id,
            CAST(token_address AS STRING) as token_address,
            token_type
        FROM 
        (
            SELECT
                output_0 as pool,
                POSEXPLODE(_coins) as (token_id, token_address),
                'pool_token' as token_type
            FROM
            {{ source('curvefi_fantom', 'StableSwap_Factory_call_deploy_plain_pool') }}
            WHERE call_success = true 
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) x 
        WHERE x.pool IS NOT NULL -- some pools are weirdly having a null output_0 so will enter them manually
),

harcoded_plainpools as ( -- these pools have a null output_o in the deploy plain pool function and they actually have a few trades so manaully importing them (only one has multiple transactions rest are 0 transactions and pools have zero liquidity)
                        -- there are also some plain pools like tricrypto that aren't deployed by factory and don't show up here https://dune.com/queries/1933132 used this query to get the most active ones 
        SELECT 
            LOWER(pool) as pool, 
            token_id, 
            LOWER(token_address) as token_address,
            'pool_token' as token_type 
        FROM (
        VALUES 
        -- FraxTUSD 4Pool pool
            ('0x872686B519E06B216EEf150dC4914f35672b0954', '0', '0x04068da6c83afcfa0e13ba15a6696662335d5b75'), -- usdc 
            ('0x872686B519E06B216EEf150dC4914f35672b0954', '1', '0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E'), -- dai
            ('0x872686B519E06B216EEf150dC4914f35672b0954', '2', '0x9879abdea01a879644185341f7af7d8343556b7a'), -- tusd
            ('0x872686B519E06B216EEf150dC4914f35672b0954', '3', '0xdc301622e621166bd8e82f2ca0a26c13ad0be355') -- frax
        ) as temp_table (pool, token_id, token_address)
),

plain_pools_pool_tokens as (
        SELECT 
            *, 
            'Plain Pool' as pool_type 
        FROM 
        plain_pools
        WHERE token_address != '0x0000000000000000000000000000000000000000' -- the 3rd & 4th coins are usually 0x000 so filtering here

        UNION ALL 

        SELECT 
            *,
            'Plain Pool' as pool_type
        FROM 
        harcoded_plainpools
), 

meta_pools as ( -- getting meta pools and their base pools 
        SELECT 
            pool, 
            CAST(token_id AS STRING) as token_id,
            CAST(token_address AS STRING) as token_address,
            base_pool
        FROM 
        (
            SELECT
                output_0 as pool,
                '0' as token_id, 
                _coin as token_address,
                _base_pool as base_pool
            FROM
            {{ source('curvefi_fantom', 'StableSwap_Factory_call_deploy_metapool') }}
            WHERE call_success = true 
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}

            UNION ALL

            SELECT
                a.output_0 as pool,
                '1' as token_id,
                b.lp_token as token_address, -- token id (1) is the lp token of the base pool
                a._base_pool as base_pool
            FROM
            {{ source('curvefi_fantom', 'StableSwap_Factory_call_deploy_metapool') }} a 
            INNER JOIN 
            base_pools_lp_tokens b 
                ON a._base_pool = b.pool
                AND a.call_success = true 
                {% if is_incremental() %}
                AND a.call_block_time >= date_trunc("day", now() - interval '1 week')
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
        WHERE token_id = '0'
        
        UNION ALL
        
        SELECT 
            mt.pool, 
            bt.token_address, 
            CASE -- reordering here as the '0' token of the metapool is first and then the pool tokens
                WHEN bt.token_id = '0' THEN '1' 
                WHEN bt.token_id = '1' THEN '2'
                WHEN bt.token_id = '2' THEN '3'
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
        WHERE token_id = '0'
        
        UNION ALL  -- duplicate here because the pool token (1) is also token 2 & 3

        SELECT 
            pool, 
            token_address, 
            token_id, 
            'underlying_token_sold' as token_type,
            'Meta Pool' as pool_type
        FROM 
        meta_pools_pool_tokens
        WHERE token_id = '1'
        
        UNION ALL

        SELECT 
            pool, 
            token_address, 
            '2' as  token_id, 
            'underlying_token_sold' as token_type,
            'Meta Pool' as pool_type
        FROM 
        meta_pools_pool_tokens
        WHERE token_id = '1'
        
        UNION ALL
        
        SELECT 
            pool, 
            token_address, 
            '3' as  token_id, 
            'underlying_token_sold' as token_type,
            'Meta Pool' as pool_type
        FROM 
        meta_pools_pool_tokens
        WHERE token_id = '1'
), 

hardcoded_pools as ( -- some pools are not decoded via factory and don't show in the plain pool or meta pool event so their details have to be entered manually https://dune.com/queries/1933132 used this query to get the most active ones, had to manually submit them for decoding to as they're not picked up by the factory
    SELECT 
        LOWER(pool) as pool, 
        token_id, 
        LOWER(token_address) as token_address,
        token_type, 
        pool_type 
    FROM (
    VALUES 
    -- tricrypto pool
        ('0x3a1659Ddcf2339Be3aeA159cA010979FB49155FF', '0', '0x049d68029688eAbF473097a2fC38ef61633A3C7A', 'pool_token', 'Plain Pool'), -- fusdt 
        ('0x3a1659Ddcf2339Be3aeA159cA010979FB49155FF', '1', '0x321162Cd933E2Be498Cd2267a90534A804051b11', 'pool_token', 'Plain Pool'), -- wbtc
        ('0x3a1659Ddcf2339Be3aeA159cA010979FB49155FF', '2', '0x74b23882a30290451A17c44f4F05243b6b58C76d', 'pool_token', 'Plain Pool'), -- weth
    -- fusdt pool tokens 
        ('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '0', '0x049d68029688eAbF473097a2fC38ef61633A3C7A', 'pool_token', 'Meta Pool'), -- fusdt 
        ('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '1', '0x27E611FD27b276ACbd5Ffd632E5eAEBEC9761E40', 'pool_token', 'Meta Pool'), -- 2pool pool 
    -- fusdt underlying sold 
        ('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '0', '0x049d68029688eAbF473097a2fC38ef61633A3C7A', 'underlying_token_sold', 'Meta Pool'), -- fusdt 
        ('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '1', '0x27E611FD27b276ACbd5Ffd632E5eAEBEC9761E40', 'underlying_token_sold', 'Plain Pool'), -- 2pool pool 
        ('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '2', '0x27E611FD27b276ACbd5Ffd632E5eAEBEC9761E40', 'underlying_token_sold', 'Plain Pool'), -- 2pool pool 
    -- fusdt underlying bought
        ('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '0', '0x049d68029688eAbF473097a2fC38ef61633A3C7A', 'underlying_token_bought', 'Meta Pool'), -- fusdt 
        ('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '1', '0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E', 'underlying_token_bought', 'Meta Pool'), -- dai
        ('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '2', '0x04068DA6C83AFCFA0e13ba15A6696662335D5B75', 'underlying_token_bought', 'Meta Pool') -- usdc
    ) as temp_table (pool, token_id, token_address, token_type, pool_type)
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
        SELECT pool, token_id, token_address, token_type, pool_type FROM hardcoded_pools
)

SELECT 
    'fantom' as blockchain, 
    'curve' as project, 
    '2' as version, 
    pool, 
    token_id, 
    token_address, 
    token_type, 
    pool_type
FROM 
all_pool_tokens

