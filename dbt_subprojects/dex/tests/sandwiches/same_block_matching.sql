-- Tie this fixture-only regression to both models for dbt's indirect test selection.
-- depends_on: {{ ref('dex_bnb_sandwiches') }}
-- depends_on: {{ ref('dex_bnb_sandwiched') }}

with fixture_rows (tx_hash, block_number, evt_index, pool, actor, bought, sold) as (
    values
        -- Positive control: one same-block sandwich.
        (0x01, 100, 1, 0x01, 0xaa, 0x11, 0x22),
        (0x02, 100, 2, 0x01, 0xbb, 0x11, 0x22),
        (0x03, 100, 4, 0x01, 0xaa, 0x22, 0x11),
        -- Back leg in a different block with the same timestamp.
        (0x04, 200, 1, 0x02, 0xaa, 0x11, 0x22),
        (0x05, 200, 2, 0x02, 0xbb, 0x11, 0x22),
        (0x06, 201, 4, 0x02, 0xaa, 0x22, 0x11),
        -- Victim in a different block, while both attacker legs share a block.
        (0x07, 300, 1, 0x03, 0xaa, 0x11, 0x22),
        (0x08, 301, 2, 0x03, 0xbb, 0x11, 0x22),
        (0x09, 300, 4, 0x03, 0xaa, 0x22, 0x11),
        -- Two valid sandwiches sharing timestamp/pool/attacker, with overlapping
        -- event indices. A third block's trade must not become a victim.
        (0x0a, 400, 1, 0x04, 0xaa, 0x11, 0x22),
        (0x0b, 400, 2, 0x04, 0xbb, 0x11, 0x22),
        (0x0c, 400, 4, 0x04, 0xaa, 0x22, 0x11),
        (0x0d, 401, 3, 0x04, 0xaa, 0x11, 0x22),
        (0x0e, 401, 4, 0x04, 0xbb, 0x11, 0x22),
        (0x0f, 401, 6, 0x04, 0xaa, 0x22, 0x11),
        (0x10, 402, 3, 0x04, 0xbb, 0x11, 0x22),
        -- Outside its own bounds but inside a false cross-block bound (1, 6).
        (0x11, 400, 5, 0x04, 0xbb, 0x11, 0x22)
), fixture_trades as (
    select 'bnb' as blockchain
    , 'fixture' as project
    , '1' as version
    , cast(current_date as timestamp) as block_time
    , cast(date_trunc('month', current_date) as date) as block_month
    , block_number
    , sold as token_sold_address
    , bought as token_bought_address
    , 'SOLD' as token_sold_symbol
    , 'BOUGHT' as token_bought_symbol
    , actor as maker
    , actor as taker
    , tx_hash
    , actor as tx_from
    , pool as tx_to
    , pool as project_contract_address
    , 'BOUGHT-SOLD' as token_pair
    , cast(1 as uint256) as token_sold_amount_raw
    , cast(1 as uint256) as token_bought_amount_raw
    , 1e0 as token_sold_amount
    , 1e0 as token_bought_amount
    , 1e0 as amount_usd
    , evt_index
    from fixture_rows
), fixture_transactions as (
    select tx_hash as hash, block_time, block_number, evt_index as index
    from fixture_trades
), actual_sandwiches as (
    {{ dex_sandwiches('bnb', 'fixture_transactions', trades='fixture_trades') }}
), actual_sandwiched as (
    {{ dex_sandwiched('bnb', 'fixture_transactions', 'actual_sandwiches', trades='fixture_trades') }}
), expected (kind, tx_hash) as (
    values ('attacker', 0x01), ('attacker', 0x03),
        ('attacker', 0x0a), ('attacker', 0x0c),
        ('attacker', 0x0d), ('attacker', 0x0f),
        ('victim', 0x02), ('victim', 0x0b), ('victim', 0x0e)
), actual as (
    select 'attacker' as kind, tx_hash from actual_sandwiches
    union all
    select 'victim' as kind, tx_hash from actual_sandwiched
), differences as (
    (select kind, tx_hash from actual except select kind, tx_hash from expected)
    union all
    (select kind, tx_hash from expected except select kind, tx_hash from actual)
)
select kind, tx_hash from differences
union all
select kind, tx_hash from actual group by 1, 2 having count(*) != 1
