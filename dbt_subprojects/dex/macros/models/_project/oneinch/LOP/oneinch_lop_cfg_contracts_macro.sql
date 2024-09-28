{% macro oneinch_lop_cfg_contracts_macro() %}

-- METHODS SAMPLES
{%
    set samples = {
        "v2": {
            "maker":         "from_hex(order_map['maker'])",
            "maker_asset":   "from_hex(order_map['makerAsset'])",
            "taker_asset":   "from_hex(order_map['takerAsset'])",
            "making_amount": "output_0",
            "taking_amount": "output_1",
        },
        "v4": {
            "maker":         "substr(cast(cast(order_map['maker'] as uint256) as varbinary), 13)",
            "receiver":      "substr(cast(cast(order_map['receiver'] as uint256) as varbinary), 13)",
            "maker_asset":   "substr(cast(cast(order_map['makerAsset'] as uint256) as varbinary), 13)",
            "taker_asset":   "substr(cast(cast(order_map['takerAsset'] as uint256) as varbinary), 13)",
            "making_amount": "output_0",
            "taking_amount": "output_1",
            "order_hash":    "output_2",
            "order_remains": "varbinary_concat(0x01, substr(cast(cast(order_map['salt'] as uint256) as varbinary), 1, 4))",
            "maker_traits":  "cast(cast(order_map['makerTraits'] as uint256) as varbinary)",
            "partial_bit":   "1",
            "multiple_bit":  "2",
        }
    }
%}
-- partial_bit & multiple_bit: the number of the bit on the left starting from 1 to 256 in 32 bytes in MakerTraits struct
-- [the number of the bit] = 256 - [solidity offset (~ PARTIAL_FILLS & ~ MULTIPLE_FILLS)]

-- CONTRACTS CONFIG
{%
    set contracts = {
        "LimitOrderProtocolV1": {
            "version": "1",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism"],
            "start": "2021-06-03",
            "methods": {
                "fillOrder":    dict(samples["v2"], maker="substr(from_hex(order_map['makerAssetData']), 4 + 12 + 1, 20)"),
                "fillOrderRFQ": dict(
                    samples["v2"],
                    maker=        "substr(from_hex(order_map['makerAssetData']), 4 + 12 + 1, 20)",
                    making_amount="bytearray_to_uint256(substr(from_hex(order_map['makerAssetData']), 4 + 32*2 + 1, 32))",
                    taking_amount="bytearray_to_uint256(substr(from_hex(order_map['takerAssetData']), 4 + 32*2 + 1, 32))",
                ),
            },
        },
        "LimitOrderProtocolV2": {
            "version": "2",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism"],
            "start": "2021-11-26",
            "methods": {
                "fillOrder":                samples["v2"],
                "fillOrderTo":              dict(samples["v2"], receiver="from_hex(order_map['receiver'])"),
                "fillOrderToWithPermit":    dict(samples["v2"], receiver="from_hex(order_map['receiver'])"),
                "fillOrderRFQ":             samples["v2"],
                "fillOrderRFQTo":           samples["v2"],
                "fillOrderRFQToWithPermit": samples["v2"],
            },
        },
        "AggregationRouterV4": {
            "version": "2",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "fantom"],
            "start": "2021-11-05",
            "methods": {
                "fillOrderRFQ":             samples["v2"],
                "fillOrderRFQTo":           samples["v2"],
                "fillOrderRFQToWithPermit": samples["v2"],
            },
        },
        "AggregationRouterV5": {
            "version": "3",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "fantom", "base", "zksync"],
            "start": "2022-11-04",
            "methods": {
                "fillOrder":                dict(samples["v2"], order_hash="output_2"),
                "fillOrderTo":              dict(
                    samples["v2"],
                    order =         '"order_"',
                    making_amount = "output_actualMakingAmount",
                    taking_amount = "output_actualTakingAmount",
                    order_hash =    "output_orderHash",
                    receiver =      "from_hex(order_map['receiver'])",
                ),
                "fillOrderToWithPermit":    dict(samples["v2"], order_hash="output_2", receiver="from_hex(order_map['receiver'])"),
                "fillOrderRFQ":             dict(samples["v2"], order_hash="output_2"),
                "fillOrderRFQTo":           dict(
                    samples["v2"],
                    making_amount = "output_filledMakingAmount",
                    taking_amount = "output_filledTakingAmount",
                    order_hash =    "output_orderHash",
                ),
                "fillOrderRFQToWithPermit": dict(samples["v2"], order_hash="output_2"),
                "fillOrderRFQCompact":      dict(
                    samples["v2"],
                    making_amount = "output_filledMakingAmount",
                    taking_amount = "output_filledTakingAmount",
                    order_hash =    "output_orderHash",
                ),
            },
        },
        "AggregationRouterV6": {
            "version": "4",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "fantom", "base"],
            "start": "2024-02-12",
            "methods": {
                "fillOrder":             samples["v4"],
                "fillOrderArgs":         dict(samples["v4"], args="args"),
                "fillContractOrder":     samples["v4"],
                "fillContractOrderArgs": samples["v4"],
            },
        },
    }
%}


{{ return(contracts) }}

{% endmacro %}