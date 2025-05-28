{{ config(
    schema = 'tamadotmeme_v1_ronin',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
) }}

{% set project_start_date = '2025-01-21 14:07' %}
{% set wron_token_address = '0x0000000000000000000000000000000000000000' %}



with trades AS (
    select 
    'ronin' AS blockchain, 
    'tamadotmeme' AS project, 
    '1' AS version, 
    CAST(DATE_TRUNC('month', evt_block_time) as date) AS block_month, 
    CAST(DATE_TRUNC('day', evt_block_time) as date) AS block_date, 
    evt_block_time AS block_time, -- Keep original block time
    evt_block_number AS block_number, 
    amountIn as token_sold_amount_raw,
    amountOut as token_bought_amount_raw,
    -- If it's a buy, the 'token' column holds bought token. If it's a sell order, Native ron is bought
    case when isBuy=True then token                    
         else {{wron_token_address}}                  
    end as token_bought_address, 
    -- If it's a buy, native RON is the sold token. It it's a sell, then the token column holds the token sold address
    case when isBuy=True then {{wron_token_address}}      
         else token                                    
    end as token_sold_address, 
     -- If it's a buy, sender is taker. If it's a sell order then the project, 0xa54b0184d12349cf65281c6f965a74828ddd9e8f is the taker
    case when isBuy=True then sender                   
         else contract_address                                       
    end as taker,
     -- If it's a buy, then the project 0xa54b0184d12349cf65281c6f965a74828ddd9e8f is maker. If it's a sell order then the user, sender is the taker
    case when isBuy=True then contract_address                      
         else sender                                   
    end as maker,
    contract_address AS project_contract_address, 
    evt_tx_hash AS tx_hash, 
    evt_index, 
    evt_tx_from AS tx_from, 
    evt_tx_to AS tx_to  

  FROM  {{ source('tamadotmeme_ronin', 'maincontract_evt_trade') }} -- Source table
  WHERE evt_block_time >= TRY_CAST('{{project_start_date}}' AS TIMESTAMP) -- Filter by project start date
    {% if is_incremental() %}
    AND
        {{ incremental_predicate('evt_block_time') }} -- Incremental load predicate
    {% endif %}
)

select
*
from trades
