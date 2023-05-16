{{ config(
    schema = 'layerzero_ethereum',
    alias = 'send',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash', 'user_address', 'trace_address', 'source_chain_id', 'destination_chain_id', 'currency_contract'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                              "project",
                              "layerzero",
                              \'["bennyfeng"]\') }}'
    )
}}

{% set transaction_start_date = "2022-03-15" %}
{% set endpoint_contract = "0x66a71dcef29a0ffbdbe3c6a460a3b5bc225cd675" %}
{% set native_token_contract = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" %}

WITH chain_list(chain_name, chain_id, endpoint_address) as (
    values
    ('Ethereum', 101, '0x66a71dcef29a0ffbdbe3c6a460a3b5bc225cd675'),
    ('BNB Chain', 102, '0x3c2269811836af69497e5f486a85d7316753cf62'),
    ('Aptos', 108, '0x54ad3d30af77b60d939ae356e6606de9a4da67583f02b962d2d3f2e481484e90'),
    ('Avalanche', 106, '0x3c2269811836af69497e5f486a85d7316753cf62'),
    ('Polygon', 109, '0x3c2269811836af69497e5f486a85d7316753cf62'),
    ('Arbitrum', 110, '0x3c2269811836af69497e5f486a85d7316753cf62'),
    ('Optimism', 111, '0x3c2269811836af69497e5f486a85d7316753cf62'),
    ('Fantom', 112, '0xb6319cc6c8c27a8f5daf0dd3df91ea35c4720dd7'),
    ('Swimmer', 114, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('DFK', 115, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Harmony', 116, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Moonbeam', 126, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Celo', 125, '0x3a73033c0b1407574c76bdbac67f126f6b4a9aa9'),
    ('Dexalot', 118, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Fuse', 138, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Gnosis', 145, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Klaytn', 150, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Metis', 151, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Intain', 152, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('CoreDAO', 153, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('OKX', 155, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Polygon zkEVM', 158, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('zkSync Era', 165, '0x9b896c0e23220469c7ae69cb4bbae391eaa4c8da'),
    ('Moonriver', 167, '0x7004396c99d5690da76a7c59057c5f3a53e01704')
),
send_call_detail AS (
    SELECT CAST(101 AS integer) AS source_chain_id,
        s.call_tx_hash as tx_hash,
        s.call_block_number as block_number,
        s._dstChainId AS destination_chain_id,
        s.contract_address,
        s.call_block_time AS block_time,
        s.call_trace_address AS trace_address,
        s._adapterParams AS adapter_params,
        s._refundAddress AS refund_address,
        s._zroPaymentAddress AS zro_payment_address,
        t.`from` AS user_address,
        t.`to` AS transaction_contract_address,
        CASE WHEN len(_destination) >= 82
            THEN '0x' || right(_destination, 40)
            ELSE '' END AS local_contract_address, -- 
        CASE WHEN len(_destination) >= 82
            THEN substring(_destination, 1, len(_destination) - 40)
            ELSE _destination END AS remote_contract_address,
        CASE WHEN et.evt_index IS NULL
            THEN '{{ native_token_contract }}'
            ELSE et.contract_address END AS token_contract_address,
        CASE WHEN et.evt_index IS NULL
            THEN t.value
            ELSE et.value END AS amount_raw
    FROM {{ source ('layerzero_ethereum', 'Endpoint_call_send') }} s
    INNER JOIN {{ source('ethereum','transactions') }} t on t.block_number = s.call_block_number
        AND t.hash = s.call_tx_hash
        {% if not is_incremental() %}
        AND t.block_time >= '{{transaction_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND t.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ source('erc20_ethereum', 'evt_transfer') }} et on et.evt_block_number = t.block_number
        AND et.evt_tx_hash = t.hash
        AND et.`from` = t.`from`
        {% if not is_incremental() %}
        AND et.evt_block_time >= '{{transaction_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND et.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    WHERE s.call_success
        {% if not is_incremental() %}
        AND s.call_block_time >= '{{transaction_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND s.call_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

transfer_amount_detail AS (
    SELECT s.source_chain_id,
        s.tx_hash,
        s.block_number,
        s.destination_chain_id,
        s.contract_address,
        s.block_time,
        s.trace_address,
        s.adapter_params,
        s.refund_address,
        s.zro_payment_address,
        s.user_address,
        s.transaction_contract_address,
        s.local_contract_address,
        s.remote_contract_address,
        s.token_contract_address,
        (s.amount_raw - t.value) AS amount_raw
    FROM send_call_detail s
    INNER JOIN {{ source('ethereum', 'traces') }} t ON t.block_number = s.block_number
        AND t.tx_hash = s.tx_hash
        AND cast(t.value as double) > 0
        AND cardinality(t.trace_address) > 0
        AND t.to = '{{ endpoint_contract }}'
        {% if not is_incremental() %}
        AND t.block_time >= '{{transaction_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND t.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    WHERE s.token_contract_address = '{{ native_token_contract }}'
    
    UNION ALL
    
    SELECT s.source_chain_id,
        s.tx_hash,
        s.block_number,
        s.destination_chain_id,
        s.contract_address,
        s.block_time,
        s.trace_address,
        s.adapter_params,
        s.refund_address,
        s.zro_payment_address,
        s.user_address,
        s.transaction_contract_address,
        s.local_contract_address,
        s.remote_contract_address,
        s.token_contract_address,
        s.amount_raw
    FROM send_call_detail s
    WHERE s.token_contract_address <> '{{ native_token_contract }}'
)

SELECT s.source_chain_id,
    cls.chain_name AS source_chain_name,
    s.destination_chain_id,
    cld.chain_name AS destination_chain_name,
    s.tx_hash,
    s.block_number,
    s.contract_address as endpoint_contract,
    date_trunc('day', s.block_time) AS block_date,
    s.trace_address,
    s.adapter_params,
    s.refund_address,
    s.zro_payment_address,
    s.block_time,
    s.user_address,
    s.transaction_contract_address AS transaction_contract,
    s.local_contract_address AS source_bridge_contract,
    s.remote_contract_address AS destination_bridge_contract,
    CASE WHEN erc.symbol = 'WETH' THEN 'ETH' ELSE erc.symbol END AS currency_symbol,
    s.token_contract_address as currency_contract,
    COALESCE(s.amount_raw,0) / power(10, erc.decimals) * p.price AS amount_usd,
    COALESCE(s.amount_raw,0) / power(10, erc.decimals) AS amount_original,
    COALESCE(s.amount_raw,0) AS amount_raw
FROM transfer_amount_detail s
LEFT JOIN chain_list cls ON cls.chain_id = s.source_chain_id
LEFT JOIN chain_list cld ON cld.chain_id = s.destination_chain_id
LEFT JOIN {{ ref('tokens_erc20') }} erc ON erc.blockchain = 'ethereum' AND erc.contract_address = s.token_contract_address
LEFT JOIN {{ source('prices', 'usd') }} p ON p.contract_address = s.token_contract_address
    AND p.minute = date_trunc('minute', s.block_time)
    {% if not is_incremental() %}
    AND p.minute >= '{{transaction_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
