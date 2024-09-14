{{ config(
    schema = 'spaceharpoon_polygon',
    alias = 'nft_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index', 'nft_token_id', 'nft_amount'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH 

fungible_transfers as ( 
    SELECT 
        CASE WHEN token_standard = 'native' THEN 0x0000000000000000000000000000000000001010
        else contract_address end as token_address, 
        "from" as wallet_address,
        amount as amount,
        tx_hash, 
        block_time
    FROM 
    {{ source('tokens_polygon', 'transfers') }} 
    WHERE token_standard IN ('native', 'erc20')
    {% if is_incremental() %}
    AND {{incremental_predicate('block_time')}}
    {% endif %}

    union all

    SELECT 
        CASE WHEN token_standard = 'native' THEN 0x0000000000000000000000000000000000001010
        else contract_address end as token_address, 
        to as wallet_address,
        -amount as amount,
        tx_hash, 
        block_time
    FROM 
    {{ source('tokens_polygon', 'transfers') }} 
    WHERE token_standard IN ('native', 'erc20')
    {% if is_incremental() %}
    AND {{incremental_predicate('block_time')}}
    {% endif %}
), 

transfers_aggregated_tmp as (
    SELECT 
        SUM(amount) as cum_amt, 
        wallet_address,
        token_address, 
        tx_hash, 
        block_time 
    FROM 
    fungible_transfers
    GROUP BY 2, 3, 4, 5
    HAVING SUM(amount_raw) > 0 
),

prices as (
    SELECT 
        date_trunc('hour', minute) as time, 
        contract_address, 
        symbol, 
        AVG(price) as price, 
        AVG(decimals) as decimals 
    FROM 
    {{ source('prices', 'usd_forward_fill') }} 
    WHERE 1 = 1
    AND blockchain = 'polygon'
    {% if is_incremental() %}
    AND {{incremental_predicate('minute')}}
    {% endif %}
    GROUP BY 1, 2, 3 
),

transfers_aggregated as (
    SELECT 
        SUM(ta.cum_amt) as amount, 
        SUM(ta.cum_amt * p.price) as amount_usd,
        array_agg(ta.wallet_address) as interacting_addresses,
        ta.tx_hash, 
        ta.block_time 
    FROM 
    transfers_aggregated_tmp ta 
    INNER JOIN 
    prices p 
        ON date_trunc('hour', ta.block_time) = p.time 
        AND ta.token_address = p.contract_address
    GROUP BY 4, 5
),

nft_transfers as (
    SELECT 
        block_time,
        block_number,
        tx_hash,
        contract_address,
        "from",
        to,
        amount,
        token_id,
        transfer_type,
        evt_index
    FROM 
    {{ source('nft', 'transfers') }} 
    WHERE 1 = 1
    AND blockchain = 'polygon'
    {% if is_incremental() %}
    AND {{incremental_predicate('block_time')}}
    {% endif %}
)

SELECT 
    'polygon' as blockchain,
    'New Methodology' as project,
    '1' as project_version,
    CAST(date_trunc('day', nt.block_time) as date) as block_date,
    CAST(date_trunc('month', nt.block_time) as date) as block_month,
    nt.block_time,
    nt.block_number,
    nt.tx_hash,
    nt.evt_index,
    tx.to as project_contract_address,
    CAST(NULL as VARCHAR) as trade_category,
    CAST(NULL as VARCHAR) as trade_type,
    CAST(NULL as VARBINARY) as buyer,
    CAST(NULL as VARBINARY) as seller,
    nt.contract_address as nft_contract_address,
    nt.token_id as nft_token_id,
    nt.amount as nft_amount,
    CAST(NULL as VARBINARY) as currency_contract,
    CAST(NULL as UINT256) as platform_fee_amount_raw,
    CAST(NULL as UINT256) as royalty_fee_amount_raw,
    CAST(NULL as VARBINARY) as platform_fee_address,
    CAST(NULL as VARBINARY) as royalty_fee_address,
    tx."from" as tx_from,
    tx.to as tx_to,
    nft.name as nft_collection,
    nft.standard as nft_standard,
    CAST(NULL as VARCHAR) as currency_symbol,
    ta.amount as price,
    CAST(NULL as double) as platform_fee_amount,
    CAST(NULL as double) as royalty_fee_amount,
    ta.amount_usd as price_usd,
    CAST(NULL as double) as platform_fee_amount_usd,
    CAST(NULL as double) as royalty_fee_amount_usd,
    CAST(NULL as double) as platform_fee_percentage,
    CAST(NULL as double) as royalty_fee_percentage,
    agg.contract_address as aggregator_address,
    CASE 
        WHEN agg.name = 'Gem' AND nt.block_number >= 16971894 THEN 'OpenSea Pro' -- 16971894 is the first block of 2023-04-04 which is when Gem rebranded to OpenSea Pro
        ELSE agg.name
    END as aggregator_name
FROM 
nft_transfers nt 
INNER JOIN 
transfers_aggregated ta 
    ON nt.tx_hash = ta.tx_hash
INNER JOIN 
{{ source('polygon', 'transactions') }} tx
    ON tx.block_number = nt.block_number
    AND tx.hash = nt.tx_hash
    {% if is_incremental() %}
    AND {{incremental_predicate('tx.block_time')}}
    {% endif %}
LEFT JOIN 
{{ source('tokens', 'nft') }}  nft
    ON nft.blockchain = 'polygon'
    AND nft.contract_address = nt.contract_address
LEFT JOIN 
{{ source('nft', 'aggregators') }} agg
    ON agg.blockchain = 'polygon'
    AND tx.to = agg.contract_address