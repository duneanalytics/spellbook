{{ config(
        schema = 'metrics'
        , alias = 'transactions_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

with evm as (
    select
        *
    from
        (
            select
                blockchain
                , cast(date_trunc('day', block_time) as date) as block_date
                , hash as tx_hash
            from
                {{ source('evms', 'transactions') }}
            where
                1 = 1
                {% if is_incremental() or true %}
                and {{ incremental_predicate('block_time') }}
                {% endif %}
        )
    union all
    select
        *
    from
        (
            select
                'tron' as blockchain
                , cast(date_trunc('day', block_time) as date) as block_date
                , hash as tx_hash
            from
                {{ source('tron', 'transactions') }}
            where
                1 = 1
                {% if is_incremental() or true %}
                and {{ incremental_predicate('block_time') }}
                {% endif %}
        )
), net_transfers_filter as (
    select
        blockchain
        , block_date
        , tx_hash
    from
        {{ ref('metrics_net_transfers') }}
    where
        1 = 1
        and net_transfer_amount_usd >= 1 --only include tx's where transfer value is at least $1
        {% if is_incremental() or true %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
), filtered_tx as (
    select
        tx.blockchain
        , tx.block_date
        , tx.tx_hash
    from
        evm as tx
    inner join
        net_transfers_filter as nt
        on tx.blockchain = nt.blockchain
        and tx.block_date = nt.block_date
        and tx.tx_hash = nt.tx_hash
), solana as (
    select
        'solana' as blockchain
        , block_date
        , id as tx_hash
    from
        {{ source('solana', 'transactions') }}
    where
        1 = 1
        and block_date is not null --200m+ rows of nulls
        {% if is_incremental() or true %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
), solana_net_transfers_filter as (
    select
        blockchain
        , block_date
        , tx_id
    from
        {{ ref('metrics_net_solana_transfers') }}
    where
        1 = 1
        and net_transfer_amount_usd >= 1 --only include tx's where transfer value is at least $1
        {% if is_incremental() or true %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
), filtered_solana_tx as (
    select
        tx.blockchain
        , tx.block_date
        , tx.tx_id as tx_hash
    from
        solana as tx
    inner join
        solana_net_transfers_filter as nt
        on tx.blockchain = nt.blockchain
        and tx.block_date = nt.block_date
        and tx.tx_id = nt.tx_id
)
select
    blockchain
    , block_date
    , count(tx_hash) as tx_count
from
    filtered_tx
group by
    blockchain
    , block_date
union all
select
    blockchain
    , block_date
    , count(tx_hash) as tx_count
from
    filtered_solana_tx
group by
    blockchain
    , block_date
