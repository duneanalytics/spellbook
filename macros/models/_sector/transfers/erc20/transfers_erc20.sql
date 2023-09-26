{% macro transfers_erc20(blockchain, erc20_evt_transfer, wrapped_token_deposit=null, wrapped_token_withdrawal=null, unique_transfer_id=false) %}

WITH


erc20_transfers  as (
        SELECT 
            'receive' as transfer_type, 
            evt_tx_hash,
            evt_index, 
            evt_block_time,
            to as wallet_address, 
            contract_address as token_address,
            CAST(value as double) as amount_raw
            {% if unique_transfer_id %}
            , 'send-' || CAST(evt_tx_hash as VARCHAR) || '-' || CAST(evt_index AS VARCHAR) || '-' || CAST("to" AS VARCHAR) as unique_transfer_id
            {% endif %}
        FROM 
        {{ erc20_evt_transfer }}
        {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}

        UNION ALL 

        SELECT 
            'send' as transfer_type, 
            evt_tx_hash,
            evt_index, 
            evt_block_time,
            "from" as wallet_address, 
            contract_address as token_address,
            -CAST(value as double) as amount_raw
            {% if unique_transfer_id %}
            , 'receive-' || CAST(evt_tx_hash as VARCHAR) || '-' || CAST(evt_index AS VARCHAR) || '-' || CAST("from" AS VARCHAR) as unique_transfer_id
            {% endif %}
        FROM 
        {{ erc20_evt_transfer }}
        {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}
)

{% if wrapped_token_deposit and wrapped_token_withdrawal %}
, wrapped_token_events as (
        SELECT 
            'deposit' as transfer_type, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time,
            dst as wallet_address, 
            contract_address as token_address, 
            CAST(wad as double)as amount_raw
            {% if unique_transfer_id %}
            , 'deposit-' || CAST(evt_tx_hash AS VARCHAR) || '-' || CAST(evt_index AS VARCHAR) || '-' || CAST(dst AS VARCHAR) as unique_transfer_id
            {% endif %}
        FROM 
        {{ wrapped_token_deposit }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}

        UNION ALL 

        SELECT 
            'withdraw' as transfer_type, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time,
            src as wallet_address, 
            contract_address as token_address, 
            -CAST(wad as double)as amount_raw
            {% if unique_transfer_id %}
            , 'withdrawn-' || CAST(evt_tx_hash AS VARCHAR) || '-' || CAST(evt_index AS VARCHAR) || '-' || CAST(src AS VARCHAR) as unique_transfer_id
            {% endif %}
        FROM 
        {{ wrapped_token_withdrawal }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}
)
{% endif %}

SELECT
    '{{blockchain}}' as blockchain, 
    transfer_type,
    evt_tx_hash, 
    evt_index,
    evt_block_time,
    CAST(date_trunc('month', evt_block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
    {% if unique_transfer_id %}
    , unique_transfer_id
    {% endif %}
FROM 
erc20_transfers

{% if wrapped_token_deposit and wrapped_token_withdrawal %}
UNION ALL 

SELECT 
    '{{blockchain}}' as blockchain, 
    transfer_type,
    evt_tx_hash, 
    evt_index,
    evt_block_time,
    CAST(date_trunc('month', evt_block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
    {% if unique_transfer_id %}
    , unique_transfer_id
    {% endif %}
FROM 
wrapped_token_events
{% endif %}

{% endmacro %}
