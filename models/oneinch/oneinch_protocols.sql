{{
    config(
        schema = 'oneinch',
        alias = 'protocols',
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['contract_address', 'blockchain', 'selector'],
        
    )
}}



{% 
    set blockchains = [
        'arbitrum',
        'avalanche_c',
        'base',
        'bnb',
        'ethereum',
        'fantom',
        'gnosis',
        'optimism',
        'polygon',
        'zksync'
    ]
%}



with

contracts as (
    select 
        address
        , contract_id
        , contract_name
        , blockchain
    from (
        select *
        from (
            values
            
              (0x11111254369792b2ca5d084ab5eea397ca8fa48b, 'AR-V1',  'Aggregation Router V1',   array['ethereum'])
            , (0x111111125434b319222cdbf8c261674adb56f3ae, 'AR-V2',  'Aggregation Router V2',   array['ethereum'])
            , (0x111111254bf8547e7183e4bbfc36199f3cedf4a1, 'AR-V2',  'Aggregation Router V2',   array['bnb'])
            , (0x11111112542d85b3ef69ae05771c2dccff4faa26, 'AR-V3',  'Aggregation Router V3',   array['ethereum', 'bnb', 'polygon', 'arbitrum', 'optimism'])
            , (0x1111111254fb6c44bac0bed2854e76f90643097d, 'AR-V4',  'Aggregation Router V4',   array['ethereum', 'bnb', 'polygon', 'gnosis', 'arbitrum', 'avalanche_c', 'fantom'])
            , (0x1111111254760f7ab3f16433eea9304126dcd199, 'AR-V4',  'Aggregation Router V4',   array['optimism'])
            , (0x1111111254eeb25477b68fb85ed929f73a960582, 'V5&V3',  'AR V5 & LOP V3',          array['ethereum', 'bnb', 'polygon', 'gnosis', 'arbitrum', 'avalanche_c', 'optimism', 'fantom', 'base'])
            , (0x6e2b76966cbd9cf4cc2fa0d76d24d5241e0abc2f, 'V5&V3',  'AR V5 & LOP V3',          array['zksync'])

            , (0x3ef51736315f52d568d6d2cf289419b9cfffe782, 'LOP-V1', 'Limit Order Protocol V1', array['ethereum'])
            , (0xe3456f4ee65e745a44ec3bcb83d0f2529d1b84eb, 'LOP-V1', 'Limit Order Protocol V1', array['bnb'])
            , (0xb707d89d29c189421163515c59e42147371d6857, 'LOP-V1', 'Limit Order Protocol V1', array['polygon', 'optimism'])
            , (0xe295ad71242373c37c5fda7b57f26f9ea1088afe, 'LOP-V1', 'Limit Order Protocol V1', array['arbitrum'])

            , (0x119c71d3bbac22029622cbaec24854d3d32d2828, 'LOP-V2', 'Limit Order Protocol V2', array['ethereum'])
            , (0x1e38eff998df9d3669e32f4ff400031385bf6362, 'LOP-V2', 'Limit Order Protocol V2', array['bnb'])
            , (0x94bc2a1c732bcad7343b25af48385fe76e08734f, 'LOP-V2', 'Limit Order Protocol V2', array['polygon'])
            , (0x54431918cec22932fcf97e54769f4e00f646690f, 'LOP-V2', 'Limit Order Protocol V2', array['gnosis'])
            , (0x7f069df72b7a39bce9806e3afaf579e54d8cf2b9, 'LOP-V2', 'Limit Order Protocol V2', array['arbitrum'])
            , (0x0f85a912448279111694f4ba4f85dc641c54b594, 'LOP-V2', 'Limit Order Protocol V2', array['avalanche_c'])
            , (0x11431a89893025d2a48dca4eddc396f8c8117187, 'LOP-V2', 'Limit Order Protocol V1', array['optimism'])
            
        ) as c(address, contract_id, contract_name, blockchains)
    ), unnest(blockchains) as blockchains(blockchain)
)


, creations as (
    {% for blockchain in blockchains %}
        select 
            address as contract_address
            , contract_id
            , contract_name
            , '{{ blockchain }}' as blockchain
            , block_time as created_at
        from {{ source(blockchain, 'creation_traces') }}
        join contracts using(address)
        where blockchain = '{{ blockchain }}'

        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)


, methods as (
    select 
        id as selector
        , split_part(signature, '(', 1) as method
        , contract_id
        , protocol
        , version
        , main
        , offsets
        , signature
    from (values

          (0xe2a7515e, 'AR-V1',  'AR',  1,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- swap
        , (0xc9b27359, 'AR-V1',  'AR',  1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- infiniteApproveIfNeeded
        , (0xae4dd0fc, 'AR-V1',  'AR',  1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- withdrawAllToken
        , (0xf88309d7, 'AR-V1',  'AR',  1,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- swap
        , (0xa4c0ed36, 'AR-V1',  'AR',  1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- onTokenTransfer
        , (0xdd62ed3e, 'AR-V1',  'AR',  1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- allowance
        , (0xf2fde38b, 'AR-V1',  'AR',  1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- transferOwnership
        , (0xf23a6e61, 'AR-V1',  'AR',  1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- onERC1155Received
        , (0xa96c400e, 'AR-V1',  'AR',  1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- marketSellOrdersProportion
        , (0xc0ee0b8a, 'AR-V1',  'AR',  1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- tokenFallback
        , (0xfcc06f8e, 'AR-V1',  'AR',  1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- marketSellOrders
        , (0x90411a32, 'AR-V1',  'AR',  1,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- swap
        , (0xe8edc816, 'AR-V1',  'AR',  1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- spender
        , (0xa9059cbb, 'AR-V1',  'AR',  1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- transfer

        , (0x095ea7b3, 'AR-V2',  'AR',  2,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- approve
        , (0xf2fde38b, 'AR-V2',  'AR',  2,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- transferOwnership
        , (0xe2a7515e, 'AR-V2',  'AR',  2,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- swap
        , (0x78e3214f, 'AR-V2',  'AR',  2,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- rescueFunds
        , (0xf88309d7, 'AR-V2',  'AR',  2,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- swap
        , (0x90411a32, 'AR-V2',  'AR',  2,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- swap
        , (0x2e95b6c8, 'AR-V2',  'AR',  2,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- unoswap
        , (0xa9059cbb, 'AR-V2',  'AR',  2,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- transfer
        , (0x34b0793b, 'AR-V2',  'AR',  2,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- discountedSwap
        , (0x7ff36ab5, 'AR-V2',  'AR',  2,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- swapExactETHForTokens
        , (0x7c025200, 'AR-V2',  'AR',  2,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- swap
        , (0xfb3bdb41, 'AR-V2',  'AR',  2,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- swapETHForExactTokens

        , (0x83197ef0, 'AR-V3',  'AR',  3,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- destroy
        , (0x6c4a483e, 'AR-V3',  'AR',  3,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- discountedSwap
        , (0x715018a6, 'AR-V3',  'AR',  3,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- renounceOwnership
        , (0x78e3214f, 'AR-V3',  'AR',  3,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- rescueFunds
        , (0x7c025200, 'AR-V3',  'AR',  3,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- swap
        , (0xf2fde38b, 'AR-V3',  'AR',  3,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- transferOwnership
        , (0x2e95b6c8, 'AR-V3',  'AR',  3,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- unoswap
        , (0xa1251d75, 'AR-V3',  'AR',  3,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- unoswapWithPermit

        , (0x825caba1, 'AR-V4',  'AR',  4,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- cancelOrderRFQ
        , (0xb0431182, 'AR-V4',  'LOP', 4,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- clipperSwap
        , (0x9994dd15, 'AR-V4',  'LOP', 4,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- clipperSwapTo
        , (0xd6a92a5d, 'AR-V4',  'LOP', 4,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- clipperSwapToWithPermit
        , (0x83197ef0, 'AR-V4',  'AR',  4,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- destroy
        , (0xd0a3b665, 'AR-V4',  'LOP', 2,  true,   map_from_entries(array[('offset_wallet', 113), ('offset_src_token', 0)])) -- fillOrderRFQ
        , (0xbaba5855, 'AR-V4',  'LOP', 2,  true,   map_from_entries(array[('offset_wallet', 113), ('offset_src_token', 0)])) -- fillOrderRFQTo
        , (0x4cc4a27b, 'AR-V4',  'LOP', 2,  true,   map_from_entries(array[('offset_wallet', 113), ('offset_src_token', 0)])) -- fillOrderRFQToWithPermit
        , (0x715018a6, 'AR-V4',  'AR',  4,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- renounceOwnership
        , (0x78e3214f, 'AR-V4',  'AR',  4,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- rescueFunds
        , (0x7c025200, 'AR-V4',  'AR',  4,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- swap
        , (0xf2fde38b, 'AR-V4',  'AR',  4,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- transferOwnership
        , (0xe449022e, 'AR-V4',  'AR',  4,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- uniswapV3Swap
        , (0xfa461e33, 'AR-V4',  'AR',  4,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- uniswapV3SwapCallback
        , (0xbc80f1a8, 'AR-V4',  'AR',  4,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- uniswapV3SwapTo
        , (0x2521b930, 'AR-V4',  'AR',  4,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- uniswapV3SwapToWithPermit
        , (0x2e95b6c8, 'AR-V4',  'AR',  4,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- unoswap
        , (0xa1251d75, 'AR-V4',  'AR',  4,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- unoswapWithPermit

        , (0x72c244a8, 'V5&V3',  'AR',  5,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- advanceNonce
        , (0x2d9a56f6, 'V5&V3',  'LOP', 3,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- cancelOrder
        , (0x825caba1, 'V5&V3',  'LOP', 3,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- cancelOrderRFQ
        , (0xbddccd35, 'V5&V3',  'LOP', 3,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- cancelOrderRFQ
        , (0x84bd6d29, 'V5&V3',  'LOP', 5,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- clipperSwap
        , (0x093d4fa5, 'V5&V3',  'LOP', 5,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- clipperSwapTo
        , (0xc805a666, 'V5&V3',  'LOP', 5,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- clipperSwapToWithPermit
        , (0x83197ef0, 'V5&V3',  'AR',  5,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- destroy
        , (0x62e238bb, 'V5&V3',  'LOP', 3,  true,   map_from_entries(array[('offset_wallet', 305), ('offset_src_token', 0)])) -- fillOrder
        , (0x3eca9c0a, 'V5&V3',  'LOP', 3,  true,   map_from_entries(array[('offset_wallet', 113), ('offset_src_token', 0)])) -- fillOrderRFQ
        , (0x9570eeee, 'V5&V3',  'LOP', 3,  true,   map_from_entries(array[('offset_wallet', 113), ('offset_src_token', 0)])) -- fillOrderRFQCompact
        , (0x5a099843, 'V5&V3',  'LOP', 3,  true,   map_from_entries(array[('offset_wallet', 113), ('offset_src_token', 0)])) -- fillOrderRFQTo
        , (0x70ccbd31, 'V5&V3',  'LOP', 3,  true,   map_from_entries(array[('offset_wallet', 113), ('offset_src_token', 0)])) -- fillOrderRFQToWithPermit
        , (0xe5d7bde6, 'V5&V3',  'LOP', 3,  true,   map_from_entries(array[('offset_wallet', 337), ('offset_src_token', 0)])) -- fillOrderTo
        , (0xd365c695, 'V5&V3',  'LOP', 3,  true,   map_from_entries(array[('offset_wallet', 369), ('offset_src_token', 0)])) -- fillOrderToWithPermit
        , (0xc53a0292, 'V5&V3',  'AR',  5,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- increaseNonce
        , (0x715018a6, 'V5&V3',  'AR',  5,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- renounceOwnership
        , (0x78e3214f, 'V5&V3',  'AR',  5,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- rescueFunds
        , (0xbd61951d, 'V5&V3',  'AR',  5,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- simulate
        , (0x12aa3caf, 'V5&V3',  'AR',  5,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- swap
        , (0xf2fde38b, 'V5&V3',  'AR',  5,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- transferOwnership
        , (0xe449022e, 'V5&V3',  'AR',  5,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- uniswapV3Swap
        , (0xfa461e33, 'V5&V3',  'AR',  5,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- uniswapV3SwapCallback
        , (0xbc80f1a8, 'V5&V3',  'AR',  5,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- uniswapV3SwapTo
        , (0x2521b930, 'V5&V3',  'AR',  5,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- uniswapV3SwapToWithPermit
        , (0x0502b1c5, 'V5&V3',  'AR',  5,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- unoswap
        , (0xf78dc253, 'V5&V3',  'AR',  5,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- unoswapTo
        , (0x3c15fd91, 'V5&V3',  'AR',  5,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- unoswapToWithPermit

        , (0x72c244a8, 'LOP-V1', 'LOP', 1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- advanceNonce
        , (0xb90b5ac7, 'LOP-V1', 'LOP', 1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- cancelOrder
        , (0x825caba1, 'LOP-V1', 'LOP', 1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- cancelOrderRFQ
        , (0xf3432b1a, 'LOP-V1', 'LOP', 1,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- fillOrder
        , (0x74785238, 'LOP-V1', 'LOP', 1,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- fillOrderRFQ
        , (0x23b872e0, 'LOP-V1', 'LOP', 1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- func_20xtkDI
        , (0x23b872df, 'LOP-V1', 'LOP', 1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- func_40aVqeY
        , (0x236872de, 'LOP-V1', 'LOP', 1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- func 50BkM4K
        , (0x23b872e1, 'LOP-V1', 'LOP', 1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- func_733NCGU
        , (0xc53a0292, 'LOP-V1', 'LOP', 1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- increaseNonce
        , (0x7f29a59d, 'LOP-V1', 'LOP', 1,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- simulateCalls

        , (0x72c244a8, 'LOP-V2', 'LOP', 2,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- advanceNonce
        , (0xb244b450, 'LOP-V2', 'LOP', 2,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- cancelOrder
        , (0x825caba1, 'LOP-V2', 'LOP', 2,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- cancelOrderRFQ
        , (0x655d13cd, 'LOP-V2', 'LOP', 2,  true,   map_from_entries(array[('offset_wallet', 273), ('offset_src_token', 0)])) -- fillOrder
        , (0xd0a3b665, 'LOP-V2', 'LOP', 2,  true,   map_from_entries(array[('offset_wallet', 113), ('offset_src_token', 0)])) -- fillOrderRFQ
        , (0xbaba5855, 'LOP-V2', 'LOP', 2,  true,   map_from_entries(array[('offset_wallet', 113), ('offset_src_token', 0)])) -- fillOrderRFQTo
        , (0x4cc4a27b, 'LOP-V2', 'LOP', 2,  true,   map_from_entries(array[('offset_wallet', 113), ('offset_src_token', 0)])) -- fillOrderRFQToWithPermit
        , (0xb2610fe3, 'LOP-V2', 'LOP', 2,  true,   map_from_entries(array[('offset_wallet', 305), ('offset_src_token', 0)])) -- fillOrder To
        , (0x6073cc20, 'LOP-V2', 'LOP', 2,  true,   map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- fillOrderToWithPermit
        , (0xc53a0292, 'LOP-V2', 'LOP', 2,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- increaseNonce
        , (0x7f29a59d, 'LOP-V2', 'LOP', 2,  false,  map_from_entries(array[('offset_wallet',   0), ('offset_src_token', 0)])) -- simulateCalls

    ) as s(id, contract_id, protocol, version, main, offsets)
    left join {{ ref('signatures') }} using(id)
)


select 
    contract_id
    , contract_address
    , contract_name
    , blockchain
    , created_at
    , selector
    , method
    , protocol
    , version
    , main
    , offsets
    , signature
from creations
left join methods using(contract_id)
order by created_at