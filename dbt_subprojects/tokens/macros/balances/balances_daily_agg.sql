{%- macro balances_daily_agg(balances_raw) %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(['day', 'address', 'token_address', 'token_standard', 'token_id']) }} as unique_key
from (
    select
        blockchain,
        cast(date_trunc('day', block_time) as date) as day,
        block_number,
        block_time,
        address,
        token_address,
        token_standard,
        token_id,
        balance_raw,
        row_number() OVER (partition by date_trunc('day', block_time), token_standard, address, token_address, token_id order by block_number desc) as row_number
    from {{ balances_raw}} balances
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
) where row_number = 1


{% endmacro %}


{%- macro balances_daily_agg_from_transfers(
    transfers,
    gas_fees_source = none,
    native_token_address = var('ETH_ERC20_ADDRESS')
) %}

with transfers_in as (
    select
        blockchain,
        block_date as day,
        block_number,
        block_time,
        "to" as address,
        contract_address as token_address,
        token_standard,
        amount
    from {{ transfers }}
    where "to" != 0x0000000000000000000000000000000000000000
    {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
    {% endif %}
),

transfers_out as (
    select
        blockchain,
        block_date as day,
        block_number,
        block_time,
        "from" as address,
        contract_address as token_address,
        token_standard,
        -1 * amount as amount
    from {{ transfers }}
    where "from" != 0x0000000000000000000000000000000000000000
    {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
    {% endif %}
),

{% if gas_fees_source is not none %}
-- gas fees can be paid in stablecoins or native token (e.g., celo allows USDT, USDC)
gas_fees as (
    select
        blockchain,
        block_date as day,
        block_number,
        block_time,
        tx_from as address,
        tx_fee_currency as token_address,
        case 
            when tx_fee_currency = {{ native_token_address }} then 'native'
            else 'erc20'
        end as token_standard,
        -1 * tx_fee as amount
    from {{ gas_fees_source }}
    where tx_from is not null
        and tx_fee > 0
    {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
    {% endif %}
),
{% endif %}

all_transfers as (
    select * from transfers_in
    union all
    select * from transfers_out
    {% if gas_fees_source is not none %}
    union all
    select * from gas_fees
    {% endif %}
),

daily_aggregated as (
    select
        blockchain,
        day,
        max(block_number) as block_number,
        max(block_time) as block_time,
        address,
        token_address,
        token_standard,
        sum(amount) as daily_net_transfer
    from all_transfers
    group by 1, 2, 5, 6, 7
),

{% if is_incremental() %}
prior_balances as (
    select
        address,
        token_address,
        token_standard,
        max_by(balance, day) as prior_balance
    from {{ this }}
    where not {{ incremental_predicate('block_time') }}
    group by 1, 2, 3
),
{% endif %}

cumulative_balances as (
    select
        d.blockchain,
        d.day,
        d.block_number,
        d.block_time,
        d.address,
        d.token_address,
        d.token_standard,
        {% if is_incremental() %}
        coalesce(p.prior_balance, 0) +
        {% endif %}
        sum(d.daily_net_transfer) over (
            partition by d.address, d.token_address
            order by d.day
            rows between unbounded preceding and current row
        ) as balance
    from daily_aggregated d
    {% if is_incremental() %}
    left join prior_balances p
        on d.address = p.address
        and d.token_address = p.token_address
        and d.token_standard = p.token_standard
    {% endif %}
)

select
    blockchain,
    day,
    block_number,
    block_time,
    address,
    token_address,
    token_standard,
    cast(null as uint256) as token_id,
    balance,
    {{ dbt_utils.generate_surrogate_key(['day', 'address', 'token_address', 'token_standard']) }} as unique_key
from cumulative_balances
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}

{% endmacro %}
