{{
    config(
        schema = 'oneinch',
        alias = 'methods',
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['contract_address', 'blockchain', 'selector'],
        
    )
}}



with
        
methods as (
    select
        selector
        , protocol
        , main
        , method
        , receiver
        , contract_id
    from (
        values
        
        -- templates
        -- extra_offset: 0 / X / X*32
        --   (0x00000001, 'AR', true, 'method', null, array[0x00])
        -- , (0x00000002, 'AR', true, 'method', map_from_entries(array[('source', 'trace-input'), ('extra_flag', '0x'), ('extra_offset', '0'), ('start', '0')]), array[0x00])
        -- , (0x00000003, 'AR', true, 'method', map_from_entries(array[('source', 'trace-input')]), array[0x00])

        -- methods
          (0x261fc7ef, 'AR', true, 'aggregate', map_from_entries(array[('source', 'tx-from')]), array[0xad63, 0xb3bc, 0xae2f])
        , (0xb1752547, 'AR', true, 'aggregate', map_from_entries(array[('source', 'tx-from')]), array[0x682d, 0xdd89])
        , (0xc3f719a0, 'AR', true, 'aggregate', map_from_entries(array[('source', 'tx-from')]), array[0xf93f])
        , (0x6c4a483e, 'AR', true, 'discountedSwap', map_from_entries(array[('source', 'tx-from')]), array[0xaa26])
        , (0x34b0793b, 'AR', true, 'discountedSwap', map_from_entries(array[('source', 'tx-from')]), array[0xf3ae, 0xf4a1])
        , (0x7c025200, 'AR', true, 'swap', map_from_entries(array[('source', 'tx-from')]), array[0x097d, 0xaa26, 0xd199, 0xf3ae, 0xf4a1])
        , (0x90411a32, 'AR', true, 'swap', map_from_entries(array[('source', 'tx-from')]), array[0xa48b, 0xf3ae, 0xf4a1])
        , (0x12aa3caf, 'AR', true, 'swap', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0xf88309d7, 'AR', true, 'swap', map_from_entries(array[('source', 'tx-from')]), array[0xf3ae, 0x6781, 0xf4a1, 0xa48b])
        , (0xe449022e, 'AR', true, 'uniswapV3Swap', map_from_entries(array[('source', 'tx-from')]), array[0xd199, 0x0582, 0x097d])
        , (0xbc80f1a8, 'AR', true, 'uniswapV3SwapTo', map_from_entries(array[('source', 'tx-from')]), array[0xd199, 0x0582, 0x097d])
        , (0x2521b930, 'AR', true, 'uniswapV3SwapToWithPermit', map_from_entries(array[('source', 'tx-from')]), array[0xd199, 0x0582, 0x097d])
        , (0x2e95b6c8, 'AR', true, 'unoswap', map_from_entries(array[('source', 'tx-from')]), array[0xaa26, 0x097d, 0xf3ae, 0xf4a1, 0xd199])
        , (0x0502b1c5, 'AR', true, 'unoswap', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0xf78dc253, 'AR', true, 'unoswapTo', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0x3c15fd91, 'AR', true, 'unoswapToWithPermit', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0xa1251d75, 'AR', true, 'unoswapWithPermit', map_from_entries(array[('source', 'tx-from')]), array[0xd199, 0x097d, 0xaa26])
        , (0x84bd6d29, 'LOP', true, 'clipperSwap', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0xb0431182, 'LOP', true, 'clipperSwap', map_from_entries(array[('source', 'tx-from')]), array[0x097d, 0xd199])
        , (0x093d4fa5, 'LOP', true, 'clipperSwapTo', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0x9994dd15, 'LOP', true, 'clipperSwapTo', map_from_entries(array[('source', 'tx-from')]), array[0x097d, 0xd199])
        , (0xc805a666, 'LOP', true, 'clipperSwapToWithPermit', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0xd6a92a5d, 'LOP', true, 'clipperSwapToWithPermit', map_from_entries(array[('source', 'tx-from')]), array[0x097d, 0xd199])
        , (0x655d13cd, 'LOP', true, 'fillOrder', map_from_entries(array[('source', 'tx-from')]), array[0xb594, 0x7187, 0x2828, 0x6362, 0x690f, 0xf2b9, 0x734f])
        , (0x62e238bb, 'LOP', true, 'fillOrder', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0xf3432b1a, 'LOP', true, 'fillOrder', map_from_entries(array[('source', 'tx-from')]), array[0xe782, 0x6857, 0x8afe, 0x84eb])
        , (0x3eca9c0a, 'LOP', true, 'fillOrderRFQ', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0xd0a3b665, 'LOP', true, 'fillOrderRFQ', map_from_entries(array[('source', 'tx-from')]), array[0x097d, 0x2828, 0x690f, 0xf2b9, 0xb594, 0x7187, 0xd199, 0x6362, 0x734f])
        , (0x74785238, 'LOP', true, 'fillOrderRFQ', map_from_entries(array[('source', 'tx-from')]), array[0xe782, 0x6857, 0x84eb, 0x8afe])
        , (0x9570eeee, 'LOP', true, 'fillOrderRFQCompact', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0x5a099843, 'LOP', true, 'fillOrderRFQTo', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0xbaba5855, 'LOP', true, 'fillOrderRFQTo', map_from_entries(array[('source', 'tx-from')]), array[0x690f, 0x097d, 0xf2b9, 0xb594, 0x734f, 0xd199, 0x7187, 0x2828, 0x6362])
        , (0x70ccbd31, 'LOP', true, 'fillOrderRFQToWithPermit', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0x4cc4a27b, 'LOP', true, 'fillOrderRFQToWithPermit', map_from_entries(array[('source', 'tx-from')]), array[0xb594, 0x6362, 0x097d, 0xd199, 0x734f, 0x7187, 0x690f, 0x2828, 0xf2b9])
        , (0xb2610fe3, 'LOP', true, 'fillOrderTo', map_from_entries(array[('source', 'tx-from')]), array[0xb594, 0x7187, 0x2828, 0x6362, 0x690f, 0xf2b9, 0x734f])
        , (0xe5d7bde6, 'LOP', true, 'fillOrderTo', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0xd365c695, 'LOP', true, 'fillOrderToWithPermit', map_from_entries(array[('source', 'tx-from')]), array[0x0582])
        , (0x6073cc20, 'LOP', true, 'fillOrderToWithPermit', map_from_entries(array[('source', 'tx-from')]), array[0xf2b9, 0x690f, 0x6362, 0x2828, 0x7187, 0x734f, 0xb594])
    ) as methods(selector, protocol, main, method, receiver, contracts), unnest(contracts) as contracts(contract_id)
)


select

    -- contracts
      project
    , contract_address
    , contract_id
    , contract_name
    , blockchain
    , ec.created_at as contract_created_at
    , creator
    
    -- methods
    , selector
    , protocol
    , main
    , coalesce(split_part(signatures.signature, '(', 1), method) as method
    , receiver
    , signature

from {{ ref('oneinch_exchange_contracts') }} ec
join methods using(contract_id)
left join {{ ref('signatures') }} as signatures on signatures.id = methods.selector
order by ec.created_at, method