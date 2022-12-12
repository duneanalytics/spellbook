{{config(alias='underlying_tokens')}}

WITH

pool_underlying as (  
    SELECT  
        LOWER(pool) as pool, 
        token_id,
        LOWER(token_address) as token_address
    FROM (
    VALUES 
    -- val3EPS 
        ('0x5b5bD8913D766D005859CE002533D4838B0Ebbb5', '1', '0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56'),
        ('0x5b5bD8913D766D005859CE002533D4838B0Ebbb5', '2', '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d'),
        ('0x5b5bD8913D766D005859CE002533D4838B0Ebbb5', '3', '0x55d398326f99059fF775485246999027B3197955'), 
    -- 3eps 
        ('0xaF4dE8E872131AE328Ce21D909C74705d3Aaf452', '1', '0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56'),
        ('0xaF4dE8E872131AE328Ce21D909C74705d3Aaf452', '2', '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d'),
        ('0xaF4dE8E872131AE328Ce21D909C74705d3Aaf452', '3', '0x55d398326f99059fF775485246999027B3197955')
    ) as temp_table (pool. token_id, token_address)
), 

join_pool_tokens as (
    SELECT 
        a.*, 
        b.pool as underlying_pool 
    FROM 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} a 
    INNER JOIN 
    pool_underlying b 
        ON a.token_address = b.pool
        AND a.token_id = '1'
), 

tokens as (
    SELECT 
        a.blockchain, 
        a.version,
        a.project, 
        a.pool, 
        a.token_id,
        a.token_address
    FROM 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} a 
    INNER JOIN 
    join_pool_tokens b 
        ON a.pool = b.pool 
        AND a.token_id = '0'

    UNION 

    SELECT 
        a.blockchain,
        a.version, 
        a.project, 
        a.pool, 
        b.token_id, 
        b.token_address
    FROM 
    join_pool_tokens a 
    INNER JOIN 
    pool_underlying b 
        ON a.underlying_pool = b.pool 
)

SELECT 
    blockchain, 
    version, 
    project,
    pool, 
    token_id, 
    token_address
FROM 
tokens 