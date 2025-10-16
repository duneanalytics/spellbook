{% macro oneinch_lo_cfg_contracts_macro() %}

-- METHODS SAMPLES --
{%
    set samples = {
        "v2": {
            "maker"         : "from_hex(order_map['maker'])",
            "maker_asset"   : "from_hex(order_map['makerAsset'])",
            "taker_asset"   : "from_hex(order_map['takerAsset'])",
            "maker_amount"  : "cast(order_map['makingAmount'] as uint256)",
            "taker_amount"  : "cast(order_map['takingAmount'] as uint256)",
            "making_amount" : "output_0",
            "taking_amount" : "output_1",
        },
        "v4": {
            "maker"         : "substr(cast(cast(order_map['maker'] as uint256) as varbinary), 13)",
            "receiver"      : "substr(cast(cast(order_map['receiver'] as uint256) as varbinary), 13)",
            "maker_asset"   : "substr(cast(cast(order_map['makerAsset'] as uint256) as varbinary), 13)",
            "taker_asset"   : "substr(cast(cast(order_map['takerAsset'] as uint256) as varbinary), 13)",
            "maker_amount"  : "cast(order_map['makingAmount'] as uint256)",
            "taker_amount"  : "cast(order_map['takingAmount'] as uint256)",
            "making_amount" : "output_0",
            "taking_amount" : "output_1",
            "order_hash"    : "output_2",
            "order_remains" : "substr(cast(cast(order_map['salt'] as uint256) as varbinary), 1, 4)",
            "maker_traits"  : "cast(cast(order_map['makerTraits'] as uint256) as varbinary)",
            "taker_traits"  : "cast(takerTraits as varbinary)",
            "partial_bit"   : "1",
            "multiple_bit"  : "2",
        }
    }
%}
-- partial_bit & multiple_bit: the number of the bit on the left starting from 1 to 256 in 32 bytes in MakerTraits struct
-- [the number of the bit] = 256 - [solidity offset (~ PARTIAL_FILLS & ~ MULTIPLE_FILLS)]

-- CONTRACTS CONFIG --
{%
    set contracts = {
        "LimitOrderProtocolV1": {
            "version": "1",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism"],
            "addresses": {
                "0x3ef51736315f52d568d6d2cf289419b9cfffe782": ["ethereum"],
                "0xe3456f4ee65e745a44ec3bcb83d0f2529d1b84eb": ["bnb"],
                "0xb707d89d29c189421163515c59e42147371d6857": ["polygon", "optimism"],
                "0xe295ad71242373c37c5fda7b57f26f9ea1088afe": ["arbitrum"],
            },
            "start": "2021-06-03",
            "methods": {
                "fillOrder":    dict(samples["v2"],
                    selector    ="0xf3432b1a",
                    maker       ="substr(from_hex(order_map['makerAssetData']), 4 + 12 + 1, 20)",
                    maker_amount="bytearray_to_uint256(substr(from_hex(order_map['makerAssetData']), 4 + 32*2 + 1, 32))",
                    taker_amount="bytearray_to_uint256(substr(from_hex(order_map['takerAssetData']), 4 + 32*2 + 1, 32))",
                ),
                "fillOrderRFQ": dict(samples["v2"],
                    selector        ="0x74785238",
                    maker           ="substr(from_hex(order_map['makerAssetData']), 4 + 12 + 1, 20)",
                    maker_amount    ="bytearray_to_uint256(substr(from_hex(order_map['makerAssetData']), 4 + 32*2 + 1, 32))",
                    taker_amount    ="bytearray_to_uint256(substr(from_hex(order_map['takerAssetData']), 4 + 32*2 + 1, 32))",
                    making_amount   ="null",
                    taking_amount   ="takingAmount",
                ),
            },
        },
        "LimitOrderProtocolV2": {
            "version": "2",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "fantom"],
            "addresses": {
                "0x119c71d3bbac22029622cbaec24854d3d32d2828": ["ethereum"],
                "0x1e38eff998df9d3669e32f4ff400031385bf6362": ["bnb"],
                "0x94bc2a1c732bcad7343b25af48385fe76e08734f": ["polygon"],
                "0x7f069df72b7a39bce9806e3afaf579e54d8cf2b9": ["arbitrum"],
                "0x0f85a912448279111694f4ba4f85dc641c54b594": ["avalanche_c"],
                "0x11431a89893025d2a48dca4eddc396f8c8117187": ["optimism"],
                "0x54431918cec22932fcf97e54769f4e00f646690f": ["gnosis"],
                "0x11dee30e710b8d4a8630392781cc3c0046365d4c": ["fantom"],
            },
            "start": "2021-11-26",
            "methods": {
                "fillOrder":                dict(samples["v2"], selector="0x655d13cd"),
                "fillOrderTo":              dict(samples["v2"], selector="0xb2610fe3", receiver="from_hex(order_map['receiver'])"),
                "fillOrderToWithPermit":    dict(samples["v2"], selector="0x6073cc20", receiver="from_hex(order_map['receiver'])"),
                "fillOrderRFQ":             dict(samples["v2"], selector="0xd0a3b665"),
                "fillOrderRFQTo":           dict(samples["v2"], selector="0xbaba5855"),
                "fillOrderRFQToWithPermit": dict(samples["v2"], selector="0x4cc4a27b"),
            },
        },
        "AggregationRouterV4": {
            "version": "2",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "fantom"],
            "addresses": {
                "0x1111111254fb6c44bac0bed2854e76f90643097d": ['ethereum','bnb','polygon','arbitrum','avalanche_c','gnosis','fantom'],
                "0x1111111254760f7ab3f16433eea9304126dcd199": ['optimism'],
            },
            "start": "2021-11-05",
            "methods": {
                "fillOrderRFQ":             dict(samples["v2"], selector="0xd0a3b665"),
                "fillOrderRFQTo":           dict(samples["v2"], selector="0xbaba5855"),
                "fillOrderRFQToWithPermit": dict(samples["v2"], selector="0x4cc4a27b"),
            },
        },
        "AggregationRouterV5": {
            "version": "3",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "fantom", "base", "zksync"],
            "addresses": {
                "0x1111111254eeb25477b68fb85ed929f73a960582": ['ethereum','bnb','polygon','arbitrum','optimism','avalanche_c','gnosis','fantom','base'],
                "0x6e2b76966cbd9cf4cc2fa0d76d24d5241e0abc2f": ['zksync'],
            },
            "start": "2022-11-04",
            "methods": {
                "fillOrder":                dict(samples["v2"], selector="0x62e238bb", order_hash="output_2"),
                "fillOrderTo":              dict(samples["v2"],
                    selector        ="0xe5d7bde6",
                    order           ='"order_"',
                    making_amount   ="output_actualMakingAmount",
                    taking_amount   ="output_actualTakingAmount",
                    order_hash      ="output_orderHash",
                    receiver        ="from_hex(order_map['receiver'])",
                ),
                "fillOrderToWithPermit":    dict(samples["v2"], selector="0xd365c695", order_hash="output_2", receiver="from_hex(order_map['receiver'])"),
                "fillOrderRFQ":             dict(samples["v2"], selector="0x3eca9c0a", order_hash="output_2"),
                "fillOrderRFQTo":           dict(samples["v2"],
                    selector="0x5a099843",
                    making_amount   ="output_filledMakingAmount",
                    taking_amount   ="output_filledTakingAmount",
                    order_hash      ="output_orderHash",
                ),
                "fillOrderRFQToWithPermit": dict(samples["v2"], selector="0x70ccbd31", order_hash="output_2"),
                "fillOrderRFQCompact":      dict(samples["v2"],
                    selector="0x9570eeee",
                    making_amount   ="output_filledMakingAmount",
                    taking_amount   ="output_filledTakingAmount",
                    order_hash      ="output_orderHash",
                ),
            },
        },
        "AggregationRouterV6": {
            "version": "4",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "fantom", "base", "zksync", "linea", "sonic", "unichain"],
            "addresses": {
                "0x111111125421ca6dc452d289314280a0f8842a65": ['ethereum','bnb','polygon','arbitrum','optimism','avalanche_c','gnosis','fantom','base','linea','sonic','unichain'],
                "0x6fd4383cb451173d5f9304f041c7bcbf27d561ff": ['zksync'],
            },
            "start": "2024-02-12",
            "streams": ["lo", "cc"],
            "methods": {
                "fillOrder":             dict(samples["v4"], selector="0x9fda64bd"),
                "fillOrderArgs":         dict(samples["v4"], selector="0xf497df75", args="args", streams=["lo", "cc"]),
                "fillContractOrder":     dict(samples["v4"], selector="0xcc713a04"),
                "fillContractOrderArgs": dict(samples["v4"], selector="0x56a75868", args="args"),
            },
        },
        "Settlement": {
            "version": "1",
            "type": "FusionV1",
            "auxiliary": "true",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "base", "fantom"],
            "addresses": {
                "0x4bc3e539aaa5b18a82f6cd88dc9ab0e113c63377": ['arbitrum'],
                "0x7731f8df999a9441ae10519617c24568dc82f697": ['avalanche_c'],
                "0x7f069df72b7a39bce9806e3afaf579e54d8cf2b9": ['base'],
                "0x1d0ae300eec4093cee4367c00b228d10a5c7ac63": ['bnb'],
                "0xa88800cd213da5ae406ce248380802bd53b47647": ['ethereum'],
                "0xa218543cc21ee9388fa1e509f950fd127ca82155": ['fantom'],
                "0xcbdb7490968d4dbf183c60fc899c2e9fbd445308": ['gnosis'],
                "0xd89adc20c400b6c45086a7f6ab2dca19745b89c2": ['optimism'],
                "0x1e8ae092651e7b14e4d0f93611267c5be19b8b9f": ['polygon'],
            },
            "start": "2022-12-23",
            "methods": {
                "settleOrders"          : dict(selector='0x0965d04b'),
                "fillOrderInteraction"  : dict(selector='0xccee33d7'),
            },
        },
    }
%}

{{ return(contracts) }}

{% endmacro %}