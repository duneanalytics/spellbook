{{ config(
    tags=['dunesql'],
    alias = alias('events'),
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'evt_type', 'evt_tx_hash', 'evt_index', 'lien_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "astaria",
                                \'["Henrystats"]\') }}'
    )
}}

-- {%- set project_start_date = '2023-04-27' %}

WITH

borrows_tmp as (
    SELECT
        *, 
        json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.lien')), '$.collateralType') AS lien_collateralType,
        from_hex(json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.lien')), '$.token')) AS lien_token,
        from_hex(json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.lien')), '$.vault')) AS lien_vault,
        json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.lien')), '$.strategyRoot') AS lien_strategyRoot,
        json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.lien')), '$.collateralId') AS lien_collateralId,
        json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.lien')), '$.details') AS lien_details,
        json_extract_scalar(JSON_PARSE(json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.lien')), '$.details')), '$.maxAmount') AS lien_details_maxAmount,
        json_extract_scalar(JSON_PARSE(json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.lien')), '$.details')), '$.rate') AS lien_details_rate,
        json_extract_scalar(JSON_PARSE(json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.lien')), '$.details')), '$.duration') AS lien_details_duration,
        json_extract_scalar(JSON_PARSE(json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.lien')), '$.details')), '$.maxPotentialDebt') AS lien_details_maxPotentialDebt,
        json_extract_scalar(JSON_PARSE(json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.lien')), '$.details')), '$.liquidationInitialAsk') AS lien_details_liquidationInitialAsk,
        json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.point')), '$.amount') AS point_amount,
        json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.point')), '$.last') AS point_last,
        json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.point')), '$.end') AS point_end,
        json_extract_scalar(JSON_PARSE(json_extract_scalar(stack, '$.point')), '$.lienId') AS point_lienId
    FROM
    {{source('astaria_ethereum', 'LienToken_evt_NewLien')}}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
), 

borrows as (
    SELECT 
        'borrow' as evt_type, 
        evt_tx_hash, 
        evt_block_number, 
        evt_index, 
        evt_block_time, 
        et."from" as borrower,
        lien_token, 
        er.symbol as lien_symbol, 
        lien_details_rate as lien_rate, 
        lien_details_duration as lien_duration, 
        CAST(point_amount as double)/1e18 as lien_amount, 
        CAST(point_amount as double) as lien_amount_raw,
        point_last as lien_start, 
        point_end as lien_end, 
        CAST(point_lienId as VARCHAR) as lien_id, 
        CAST(lien_collateralId as VARCHAR) as lien_collateral_id, 
        b.contract_address
    FROM 
    borrows_tmp b 
    INNER JOIN 
    {{ source('ethereum','transactions') }} et 
        ON b.evt_block_number = et.block_number
        AND b.evt_tx_hash = et.hash
    {% if not is_incremental() %}
        AND et.block_time >= CAST('2023-04-27' as timestamp)
    {% endif %}
    {% if is_incremental() %}
        AND et.block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
    INNER JOIN 
    {{ ref('tokens_ethereum_erc20') }} er 
        ON  b.lien_token = er.contract_address
), 

borrows_join as (
    SELECT  
        evt_type, 
        evt_tx_hash,
        evt_block_number,
        evt_index,
        evt_block_time,
        borrower,
        lien_token,
        lien_symbol, 
        lien_rate,
        lien_duration,
        lien_amount,
        lien_amount_raw,
        lien_start,
        lien_end,
        lien_id,
        lien_collateral_id,
        contract_address
    FROM 
    borrows 
    {% if is_incremental() %}
    UNION 

    SELECT 
        evt_type, 
        evt_tx_hash,
        evt_block_number,
        evt_index,
        evt_block_time,
        borrower,
        lien_token,
        lien_symbol, 
        lien_rate,
        lien_duration,
        lien_amount,
        lien_amount_raw,
        lien_start,
        lien_end,
        lien_id,
        lien_collateral_id,
        contract_address
    FROM 
    {{this}}
    {% endif %}
), 

repays_table as (
    SELECT 
        * 
    FROM 
    {{source('astaria_ethereum', 'LienToken_evt_Payment')}}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
),

repays_calls as (
    SELECT 
        * 
    FROM 
    {{source('astaria_ethereum', 'LienToken_call_makePayment')}}
    WHERE 1 = 1 
    AND call_success = true 
), 

liquidation_table as (
    SELECT 
        * 
    FROM 
    {{source('astaria_ethereum', 'AstariaRouter_evt_Liquidation')}}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
),

repays as (
    SELECT 
        'repay' as evt_type, 
        r.evt_tx_hash, 
        r.evt_block_number, 
        r.evt_index, 
        r.evt_block_time, 
        b.borrower, 
        b.lien_token, 
        b.lien_symbol, 
        b.lien_rate, 
        b.lien_duration, 
        CAST(r.amount as double)/1e18 as lien_amount, 
        CAST(r.amount as double) as lien_amount_raw,
        b.lien_start, 
        b.lien_end, 
        CAST(r.lienId as VARCHAR) as lien_id, 
        b.lien_collateral_id,
        r.contract_address
    FROM 
    repays_table r 
    INNER JOIN 
    borrows_join b 
        ON b.evt_type = 'borrow'
        AND CAST(r.lienId as VARCHAR) = b.lien_id
    INNER JOIN (
            SELECT 
                MAX(b.evt_block_number) as borrow_block_number, 
                r.evt_block_number,
                CAST(r.lienId as VARCHAR) as lien_id 
            FROM 
            repays_table r 
            INNER JOIN 
            borrows_join b 
                ON b.evt_type = 'borrow'
                AND CAST(r.lienId as VARCHAR) = b.lien_id
                AND r.evt_block_number >= b.evt_block_number
            WHERE r.evt_tx_hash IN (SELECT call_tx_hash FROM repays_calls )
            GROUP BY 2, 3 
        ) a 
        ON CAST(r.lienId as VARCHAR) = a.lien_id
        AND b.evt_block_number = a.borrow_block_number
        AND r.evt_block_number = a.evt_block_number
    WHERE r.evt_tx_hash IN (SELECT call_tx_hash FROM repays_calls)
), 

repays_join as (
    SELECT  
        lien_id, 
        lien_start
    FROM 
    repays 
    {% if is_incremental() %}
    UNION 

    SELECT 
        lien_id, 
        lien_start
    FROM 
    {{this}}
    {% endif %}
), 

liquidation_tmp as (
    SELECT 
        'liquidation' as evt_type, 
        l.evt_tx_hash,
        l.evt_block_number,
        l.evt_index, 
        l.evt_block_time,
        b.borrower, 
        b.lien_token, 
        b.lien_symbol, 
        b.lien_rate, 
        b.lien_duration, 
        b.lien_amount, 
        b.lien_amount_raw,
        b.lien_start, 
        b.lien_end, 
        b.lien_id, 
        CAST(l.collateralId as VARCHAR) as lien_collateral_id,
        l.contract_address
        -- CONCAT(CAST(l.evt_tx_hash AS VARCHAR), CAST(b.lien_id AS VARCHAR), CAST(b.lien_start as VARCHAR)) as unique_id 
    FROM 
    liquidation_table l 
    INNER JOIN 
    borrows_join b 
        ON b.evt_type = 'borrow'
        AND CAST(collateralId as VARCHAR) = b.lien_collateral_id
        AND l.evt_block_number > b.evt_block_number
        AND l.evt_block_time > from_unixtime(CAST(b.lien_start AS DOUBLE))
    WHERE CONCAT(CAST(b.lien_id as VARCHAR), CAST(b.lien_start as VARCHAR)) NOT IN (SELECT CONCAT(CAST(lien_id as VARCHAR), CAST(lien_start as VARCHAR)) FROM repays_join WHERE evt_type = 'repay')
), 

liquidation as (
    SELECT 
        evt_type,
        evt_tx_hash,
        evt_block_number, 
        evt_index,
        evt_block_time,
        borrower,
        lien_token, 
        lien_symbol, 
        lien_rate, 
        lien_duration, 
        lien_amount, 
        lien_amount_raw,
        lien_start,
        lien_end, 
        lien_id, 
        lien_collateral_id,
        contract_address
    FROM 
    (
    SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY evt_tx_hash, lien_id, lien_collateral_id ORDER BY lien_start DESC) as rank_ 
    FROM 
    liquidation_tmp
    )
    WHERE rank_ = 1
),

all_events as (

SELECT * FROM borrows 

UNION ALL 

SELECT * FROM repays

UNION ALL

SELECT * FROM liquidation
)

SELECT 
    'ethereum' as blockchain, 
    'astaria' as project,
    '1' as version, 
    CAST(date_trunc('DAY', ae.evt_block_time) AS date) as block_date,
    CAST(date_trunc('MONTH', ae.evt_block_time) AS date) as block_month,
    ae.*, 
    d.nft_symbol, 
    d.nft_token_standard,
    d.collateral_token_contract as nft_contract_address, 
    CASE WHEN d.collateral_token_contract = 0x9ff70d528830e47154224dc5c185e4d052d0fb99 THEN 'Bad Trip' ELSE d.nft_collection END as nft_collection, 
    CAST(d.collateral_token_id as VARCHAR) as nft_token_id 
FROM 
all_events ae 
INNER JOIN 
{{ ref('astaria_ethereum_daily_deposits') }} d
    ON ae.lien_collateral_id = CAST(d.collateral_id as VARCHAR)
    AND CAST(date_trunc('DAY', ae.evt_block_time) AS date) = d.day