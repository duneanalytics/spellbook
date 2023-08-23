{{ config(
    tags=['dunesql'],
    alias = alias('lending'),
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'evt_type', 'tx_hash', 'evt_index', 'lien_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "astaria",
                                \'["Henrystats"]\') }}'
    )
}}

{%- set project_start_date = '2023-04-27' %}

SELECT 
    ae.blockchain, 
    ae.project, 
    ae.version, 
    ae.block_date, 
    CAST(date_trunc('month', ae.block_date) as date) as block_month, 
    ae.evt_block_time as block_time, 
    ae.evt_block_number as block_number, 
    ae.nft_token_id as token_id, 
    ae.nft_collection as collection, 
    ae.lien_amount * p.price as amount_usd, 
    ae.nft_token_standard as token_standard, 
    ae.evt_type, 
    ae.borrower as address, 
    ae.lien_amount as amount_original, 
    ae.lien_amount_raw as amount_raw,
    ae.lien_symbol as collateral_currency_symbol,
    ae.lien_token as collateral_currency_contract, 
    ae.nft_contract_address, 
    ae.contract_address as project_contract_address, 
    ae.evt_tx_hash as tx_hash, 
    et."from" as tx_from, 
    et."to" as tx_to, 
    ae.lien_id,
    ae.evt_index
FROM 
{{ ref('astaria_ethereum_events') }} ae 
INNER JOIN 
{{ source('ethereum', 'transactions') }} et
    ON ae.evt_tx_hash = et.hash
    {% if not is_incremental() %}
    AND et.block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND et.block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
LEFT JOIN 
{{ source('prices', 'usd') }} p
    ON p.minute = date_trunc('minute', ae.evt_block_time)
    AND p.contract_address = ae.lien_token
    AND p.blockchain = 'ethereum'
    {% if not is_incremental() %}
    AND p.minute >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' Day)
    {% endif %}