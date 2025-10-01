{%- macro transfers_from_traces_base_macro(blockchain, easy_dates=false) -%}

-- this stream process all kind of transfers from traces: native, erc20 transfer/transferFrom, mint/burn, wrapped deposit/withdrawal

{%- set token_standard = 'bep20' if blockchain == 'bnb' else 'erc20' -%}
{%- set null_address = '0x0000000000000000000000000000000000000000' -%}

{%- set transfer_selector = '0xa9059cbb' -%}
{%- set transferFrom_selector = '0x23b872dd' -%}
{%- set mint_selector = '0x40c10f19' -%}{# for DAI, etc. #}
{%- set burn_selector = '0x9dc29fac' -%}{# for DAI, etc. #}
{%- set deposit_selector = '0xd0e30db0' -%}{# for wrappers #}
{%- set withdraw_selector = '0x2e1a7d4d' -%}{# for wrappers #}
{%- set selector = 'substr(input, 1, 4)' -%}

-- output --

select
    '{{ blockchain }}' as blockchain
    , cast(date_trunc('month', block_date) as date) as block_month
    , block_date
    , block_time
    , block_number
    , tx_hash
    , trace_address
    , case
        when {{ selector }} = {{ transfer_selector }} then 'transfer'
        when {{ selector }} = {{ transferFrom_selector }} then 'transferFrom'
        when {{ selector }} = {{ mint_selector }} then 'mint'
        when {{ selector }} = {{ burn_selector }} then 'burn'
        when {{ selector }} = {{ deposit_selector }} then 'deposit'
        when {{ selector }} = {{ withdraw_selector }} then 'withdraw'
        else 'native'
    end as type 
    , if(value > uint256 '0', 'native', '{{ token_standard }}') as token_standard
    , if(value > uint256 '0', native_address, "to") as contract_address 
    , case
        when {{ selector }} in ({{ transfer_selector }}, {{ mint_selector }}, {{ burn_selector }}) then bytearray_to_uint256(substr(input, 37, 32)) -- transfer, mint, burn
        when {{ selector }} = {{ transferFrom_selector }} then bytearray_to_uint256(substr(input, 69, 32)) -- transferFrom
        when {{ selector }} = {{ withdraw_selector }} then bytearray_to_uint256(substr(input, 5, 32)) -- withdraw
        else value -- native, deposit
    end as amount_raw
    , case
        when {{ selector }} in ({{ transferFrom_selector }}, {{ burn_selector }}) then substr(input, 17, 20) -- transferFrom, burn
        when {{ selector }} = {{ mint_selector }} then {{ null_address }} -- mint
        when {{ selector }} = {{ withdraw_selector }} then "from" -- withdraw
        else "from" -- transfer, native, deposit
    end as "from"
    , case
        when {{ selector }} in ({{ transfer_selector }}, {{ mint_selector }}) then substr(input, 17, 20) -- transfer, mint
        when {{ selector }} = {{ transferFrom_selector }} then substr(input, 49, 20) -- transferFrom
        when {{ selector }} = {{ burn_selector }} then {{ null_address }} -- burn
        when {{ selector }} = {{ withdraw_selector }} then "to" -- withdraw
        else coalesce("to", address) -- native, deposit (address is used in cases of contract creation with transfer where "to" is null)
    end as "to"
    , keccak(to_utf8(concat_ws('|'
        , '{{ blockchain }}'
        , cast(block_number as varchar)
        , cast(tx_hash as varchar)
        , array_join(trace_address, '')
        , cast(if(value > uint256 '0', native_address, "to") as varchar)
    ))) as unique_key
from {{ source(blockchain, 'traces') }}, (select token_address as native_address from {{ source('dune', 'blockchains') }} where name = '{{ blockchain }}') as meta
where
    (
        length(input) >= 68 and {{ selector }} in ({{ transfer_selector }}, {{ mint_selector }}, {{ burn_selector }}) -- transfer, mint, burn
        or length(input) >= 100 and {{ selector }} = {{ transferFrom_selector }} -- transferFrom
        or length(input) >= 36 and {{ selector }} = {{ withdraw_selector }} -- withdraw
        or value > uint256 '0' -- native, deposit
    )
    and (call_type = 'call' or type = 'create') -- call_type should be only call if present; type = 'create' is contract creation
    and (tx_success or tx_success is null) -- tx_success is null - is for old ethereum data
    and success
    {% if easy_dates %}and block_date > current_date - interval '10' day{% endif %} -- easy_dates mode for dev, to prevent full scan
    {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}



{%- endmacro -%}