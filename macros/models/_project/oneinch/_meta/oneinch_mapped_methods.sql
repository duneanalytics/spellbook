{% macro oneinch_mapped_methods_macro(blockchain) %}


with
    
static as (
    select
          array['swap', 'settle', 'change', 'exact', 'batch', 'trade', 'sell', 'buy', 'fill', 'route', 'zap', 'symbiosis', 'aggregate', 'multicall', 'execute', 'wrap', 'transform'] as suitable
        , array['add', 'remove', 'mint', 'increase', 'decrease', 'cancel', 'destroy', 'claim', 'rescue', 'withdraw', 'simulate', 'join', 'exit', 'interaction', '721', '1155', 'nft', 'create'] as exceptions
        , array['fill', 'order'] as limits
        , array['1inch', 'CoWSwap', 'MetaMask', 'Odos', 'OpenOcean', 'Paraswap', 'SlingshotFinance', 'Uniswap', 'ZeroEx'] as main
        , array['Rainbow'] as notlimits
        , array[
            0x13d79a0b -- CoW settle
            , 0x0965d04b -- 1inch settleOrders
            , 0x05afc977 -- UniswapX execute
            , 0x6f1d5f51 -- UniswapX executeBatch
            , 0x3f62192e -- UniswapX execute
            , 0x0d7a16c3 -- UniswapX executeBatch
            , 0x0d335884 -- UniswapX executeWithCallback
            , 0x13fb72c7 -- UniswapX executeBatchWithCallback
        ] as intents
)

, contracts as (
    select
        '{{blockchain}}' as blockchain
        , address
        , any_value(project) as project
        , any_value(tag) as tag
        , any_value(flags) as flags
        , any_value(last_created_at) as last_created_at
        , max_by(abi, created_at) as abi
        , max_by(namespace, created_at) as namespace
        , max_by(name, created_at) as name
    from {{ source(blockchain, 'contracts') }}
    join ({{ oneinch_mapped_contracts_macro(blockchain) }}) using(address)
    where flags['user']
    group by 1, 2
)

, descriptions as (
    select
        blockchain
        , address
        , project
        , json_value(entity, 'lax $.name') as method
        , cast(json_parse(json_query(entity, 'lax $.inputs.type' with array wrapper)) as array(varchar)) as types
        , cast(json_parse(json_query(entity, 'lax $.inputs.components.type' with array wrapper)) as array(varchar)) as components
        , cast(json_parse(json_query(entity, 'lax $.inputs.components.size()' with array wrapper)) as array(int)) as sizes
        , json_query(entity, 'lax $.inputs') as inputs
        , json_query(entity, 'lax $.outputs') as outputs
        , namespace
        , name
        , tag
        , flags
        , last_created_at
    from contracts, unnest(abi) as abi(entity)
    where
        json_value(entity, 'lax $.type') = 'function'
        and json_value(entity, 'lax $.stateMutability') in ('payable', 'nonpayable')
)

, signatures as (
    select
        *
        , substr(keccak(to_utf8(signature)), 1, 4) as selector
        , reduce(suitable, false, (r, x) -> if(position(x in lower(replace(method, '_'))) > 0, true, r), r -> r) and not reduce(exceptions, false, (r, x) -> if(position(x in lower(replace(method, '_'))) > 0, true, r), r -> r) as swap
        , reduce(limits, false, (r, x) -> if(position(x in lower(replace(method, '_'))) > 0, true, r), r -> r) and not reduce(exceptions, false, (r, x) -> if(position(x in lower(replace(method, '_'))) > 0, true, r) and not contains(notlimits, project), r -> r) as _limits
    from (
        select
            *
            , method || '(' || if(
                parts is null
                , array_join(types, ',')
                , reduce(
                    sequence(1, cardinality(parts))
                    , (array_join(types, ','), parts)
                    , (r, x) -> (
                            substr(r[1], 1, position('tuple' in r[1]) - 1) || '(' || r[2][1] || ')' || substr(r[1], position('tuple' in r[1]) + 5)
                            , slice(r[2], 2, cardinality(r[2]) - 1)
                        )
                    , r -> r[1]
                )
            ) || ')' as signature
        from (
            select
                *
                , reduce(sequence(1, cardinality(sizes)), cast(array[] as array(varchar)), (r, x) -> r || array_join(slice(components, start[x], sizes[x]), ','), r -> r) as parts
            from (
                select
                    *
                    , reduce(sequence(1, cardinality(sizes)), cast(array[] as array(int)), (r, x) -> if(x = 1, r || x, r || r[x - 1] + sizes[x - 1]), r -> r) as start
                from descriptions
            )
        )
        join static on true
    )
)

-- output --

select
    map_concat(flags, map_from_entries(array[('swap', swap), ('limits', _limits), ('intents', contains(intents, selector)), ('multi', flags['multi']), ('main', contains(main, project))])) as flags
    , blockchain
    , address
    , last_created_at
    , project
    , method
    , signature
    , selector
    , namespace
    , name
    , tag
    , inputs
    , outputs
from signatures
order by project, blockchain, last_created_at, tag, address, method

{% endmacro %}