{% macro oneinch_ar_cfg_contracts_macro() %}

{%
    set samples = {
        "aggregate": {
            "src_token_address":    "fromToken",
            "dst_token_address":    "toToken",
            "src_token_amount":     "tokensAmount",
            "dst_token_amount":     "output_returnAmount",
            "dst_token_amount_min": "minTokensAmount",
            "router_type":          "generic",
        },
        "swap": {
            "kit":                  "cast(json_parse(desc) as map(varchar, varchar))",
            "src_token_address":    "from_hex(kit['srcToken'])",
            "dst_token_address":    "from_hex(kit['dstToken'])",
            "src_receiver":         "from_hex(kit['srcReceiver'])",
            "dst_receiver":         "from_hex(kit['dstReceiver'])",
            "src_token_amount":     "cast(kit['amount'] as uint256)",
            "dst_token_amount":     "output_returnAmount",
            "dst_token_amount_min": "cast(kit['minReturnAmount'] as uint256)",
            "router_type":          "generic",
        },
        "unoswap v3-v5": {
            "pools":                "pools",
            "src_token_amount":     "amount",
            "dst_token_amount":     "output_returnAmount",
            "dst_token_amount_min": "minReturn",
            "direction_mask":       "bytearray_to_uint256(rpad(0x80, 32, 0x00))",
            "unwrap_mask":          "bytearray_to_uint256(rpad(0x40, 32, 0x00))",
            "router_type":          "unoswap",
        },
        "unoswap v6": {
            "src_token_address":    "substr(cast(if(token <> 0, token) as varbinary), 13)",
            "src_token_amount":     "amount",
            "dst_token_amount":     "output_returnAmount",
            "dst_token_amount_min": "minReturn",
            "pool_type_mask":       "bytearray_to_uint256(rpad(0xe0000000, 32, 0x00))",
            "pool_type_offset":     "253",
            "direction_mask":       "bytearray_to_uint256(rpad(0x00800000, 32, 0x00))",
            "unwrap_mask":          "bytearray_to_uint256(rpad(0x10000000, 32, 0x00))",
            "src_token_mask":       "bytearray_to_uint256(rpad(0x000000ff, 32, 0x00))",
            "src_token_offset":     "224",
            "dst_token_mask":       "bytearray_to_uint256(rpad(0x0000ff00, 32, 0x00))",
            "dst_token_offset":     "232",
            "router_type":          "unoswap",
        },
        "clipper": {
            "src_token_address":    "srcToken",
            "dst_token_address":    "dstToken",
            "src_token_amount":     "inputAmount",
            "dst_token_amount":     "output_returnAmount",
            "router_type":          "clipper",
        },
    }
%}

{% set native = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}

{%
    set contracts = {
        "ExchangeV1": {
            "version": "0.1",
            "blockchains": ["ethereum"],
            "addresses": {"0xe4c577bdec9ce0f6c54f2f82aed5b1913b71ae2f": ['ethereum']},
            "start": "2019-06-03",
            "end": "2020-09-18",
            "methods": {
                "aggregate": dict(samples["aggregate"], selector='0x261fc7ef'),
            },
        },
        "ExchangeV2": {
            "version": "0.2",
            "blockchains": ["ethereum"],
            "addresses": {"0x0000000006adbd7c01bc0738cdbfc3932600ad63": ['ethereum']},
            "start": "2019-06-10",
            "end": "2020-09-18",
            "methods": {
                "aggregate": dict(samples["aggregate"], selector='0x261fc7ef'),
            },
        },
        "ExchangeV3": {
            "version": "0.3",
            "blockchains": ["ethereum"],
            "addresses": {"0x0000000053d411becdb4a82d8603edc6d8b8b3bc": ['ethereum']},
            "start": "2019-06-18",
            "end": "2020-09-18",
            "methods": {
                "aggregate": dict(samples["aggregate"], selector='0x261fc7ef'),
            },
        },
        "ExchangeV4": {
            "version": "0.4",
            "blockchains": ["ethereum"],
            "addresses": {"0x000005edbbc1f258302add96b5e20d3442e5dd89": ['ethereum']},
            "start": "2019-07-18",
            "end": "2020-09-18",
            "methods": {
                "aggregate": dict(samples["aggregate"], selector='0xb1752547'),
            },
        },
        "ExchangeV5": {
            "version": "0.5",
            "blockchains": ["ethereum"],
            "addresses": {"0x0000000f8ef4be2b7aed6724e893c1b674b9682d": ['ethereum']},
            "start": "2019-07-18",
            "end": "2020-09-18",
            "methods": {
                "aggregate": dict(samples["aggregate"], selector='0xb1752547'),
            },
        },
        "ExchangeV6": {
            "version": "0.6",
            "blockchains": ["ethereum"],
            "addresses": {"0x111112549cfedf7822eb11fbd8fd485d8a10f93f": ['ethereum']},
            "start": "2019-07-19",
            "end": "2020-09-18",
            "methods": {
                "aggregate": dict(samples["aggregate"], selector='0xc3f719a0'),
            },
        },
        "ExchangeV7": {
            "version": "0.7",
            "blockchains": ["ethereum"],
            "addresses": {"0x111111254b08ceeee8ad6ca827de9952d2a46781": ['ethereum']},
            "start": "2019-09-17",
            "end": "2019-09-29",
            "methods": {
                "swap": dict(samples["aggregate"], selector='0xf88309d7', src_token_amount="fromTokenAmount", dst_token_amount_min="minReturnAmount"),
            },
        },
        "AggregationRouterV1": {
            "version": "1",
            "blockchains": ["ethereum"],
            "addresses": {"0x11111254369792b2ca5d084ab5eea397ca8fa48b": ['ethereum']},
            "start": "2019-09-28",
            "methods": {
                "swap": dict(samples["aggregate"], selector='0xf88309d7', src_token_amount="fromTokenAmount", dst_token_amount_min="minReturnAmount"),
            },
        },
        "AggregationRouterV2": {
            "version": "2",
            "blockchains": ["ethereum", "bnb"],
            "addresses": {
                "0x111111125434b319222cdbf8c261674adb56f3ae": ['ethereum'],
                "0x111111254bf8547e7183e4bbfc36199f3cedf4a1": ['bnb'],
            },
            "start": "2020-11-04",
            "methods": {
                "swap":           dict(samples["swap"], selector='0x90411a32'),
                "discountedSwap": dict(samples["swap"], selector='0x34b0793b'),
            },
        },
        "AggregationRouterV3": {
            "version": "3",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism"],
            "addresses": {"0x11111112542d85b3ef69ae05771c2dccff4faa26": ['ethereum','bnb','polygon','arbitrum','optimism']},
            "start": "2021-03-14",
            "methods": {
                "swap":              dict(samples["swap"], selector='0x7c025200'),
                "discountedSwap":    dict(samples["swap"], selector='0x6c4a483e'),
                "unoswap":           dict(samples["unoswap v3-v5"], selector='0x2e95b6c8', blockchains=["ethereum","bnb","polygon","arbitrum"], src_token_address="srcToken", pools="transform(_0, x -> bytearray_to_uint256(x))"),
                "unoswapWithPermit": dict(samples["unoswap v3-v5"], selector='0xa1251d75', blockchains=["ethereum","bnb","polygon","arbitrum"], src_token_address="srcToken", pools="transform(pools, x -> bytearray_to_uint256(x))"),
            },
        },
        "AggregationRouterV4": {
            "version": "4",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom"],
            "addresses": {
                "0x1111111254fb6c44bac0bed2854e76f90643097d": ['ethereum','bnb','polygon','arbitrum','avalanche_c','gnosis','fantom'],
                "0x1111111254760f7ab3f16433eea9304126dcd199": ['optimism'],
            },
            "start": "2021-11-05",
            "methods": {
                "swap":                      dict(samples["swap"], selector='0x7c025200'),
                "discountedSwap":            dict(samples["swap"], selector='0x6c4a483e', blockchains=["bnb", "polygon"]),
                "clipperSwap":               dict(samples["clipper"], selector='0xb0431182', src_token_amount="amount", dst_token_amount_min="minReturn", blockchains=["ethereum"]),
                "clipperSwapTo":             dict(samples["clipper"], selector='0x9994dd15', src_token_amount="amount", dst_token_amount_min="minReturn", blockchains=["ethereum"], dst_receiver="recipient"),
                "clipperSwapToWithPermit":   dict(samples["clipper"], selector='0xd6a92a5d', src_token_amount="amount", dst_token_amount_min="minReturn", blockchains=["ethereum"], dst_receiver="recipient"),
                "unoswap":                   dict(samples["unoswap v3-v5"], selector='0x2e95b6c8', src_token_address="srcToken", pools="transform(pools, x -> bytearray_to_uint256(x))"),
                "unoswapWithPermit":         dict(samples["unoswap v3-v5"], selector='0xa1251d75', src_token_address="srcToken", pools="transform(pools, x -> bytearray_to_uint256(x))"),
                "uniswapV3Swap":             dict(samples["unoswap v3-v5"], selector='0xe449022e', unwrap_mask="bytearray_to_uint256(rpad(0x20, 32, 0x00))"),
                "uniswapV3SwapTo":           dict(samples["unoswap v3-v5"], selector='0xbc80f1a8', dst_receiver="recipient", unwrap_mask="bytearray_to_uint256(rpad(0x20, 32, 0x00))"),
                "uniswapV3SwapToWithPermit": dict(samples["unoswap v3-v5"], selector='0x2521b930', dst_receiver="recipient", unwrap_mask="bytearray_to_uint256(rpad(0x20, 32, 0x00))"),
            },
        },
        "AggregationRouterV5": {
            "version": "5",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base", "zksync"],
            "addresses": {
                "0x1111111254eeb25477b68fb85ed929f73a960582": ['ethereum','bnb','polygon','arbitrum','optimism','avalanche_c','gnosis','fantom','base'],
                "0x6e2b76966cbd9cf4cc2fa0d76d24d5241e0abc2f": ['zksync'],
            },
            "start": "2022-11-04",
            "methods": {
                "swap":                      dict(samples["swap"], selector='0x12aa3caf'),
                "clipperSwap":               dict(samples["clipper"], selector='0x84bd6d29'),
                "clipperSwapTo":             dict(samples["clipper"], selector='0x093d4fa5', dst_receiver="recipient"),
                "clipperSwapToWithPermit":   dict(samples["clipper"], selector='0xc805a666', dst_receiver="recipient"),
                "unoswap":                   dict(samples["unoswap v3-v5"], selector='0x0502b1c5', src_token_address="srcToken"),
                "unoswapTo":                 dict(samples["unoswap v3-v5"], selector='0xf78dc253', src_token_address="srcToken", dst_receiver="recipient"),
                "unoswapToWithPermit":       dict(samples["unoswap v3-v5"], selector='0x3c15fd91', src_token_address="srcToken", dst_receiver="recipient"),
                "uniswapV3Swap":             dict(samples["unoswap v3-v5"], selector='0xe449022e', unwrap_mask="bytearray_to_uint256(rpad(0x20, 32, 0x00))"),
                "uniswapV3SwapTo":           dict(samples["unoswap v3-v5"], selector='0xbc80f1a8', dst_receiver="recipient", unwrap_mask="bytearray_to_uint256(rpad(0x20, 32, 0x00))"),
                "uniswapV3SwapToWithPermit": dict(samples["unoswap v3-v5"], selector='0x2521b930', dst_receiver="recipient", unwrap_mask="bytearray_to_uint256(rpad(0x20, 32, 0x00))"),
            },
        },
        "AggregationRouterV6": {
            "version": "6",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base", "zksync", "linea", "sonic", "unichain"],
            "addresses": {
                "0x111111125421ca6dc452d289314280a0f8842a65": ['ethereum','bnb','polygon','arbitrum','optimism','avalanche_c','gnosis','fantom','base','linea','sonic','unichain'],
                "0x6fd4383cb451173d5f9304f041c7bcbf27d561ff": ['zksync'],
            },
            "start": "2024-02-12",
            "methods": {
                "swap":          dict(samples["swap"], selector='0x07ed2379', src_token_amount="output_spentAmount"),
                "clipperSwap":   dict(samples["clipper"], selector='0xd2d374e5', src_token_address="substr(cast(srcToken as varbinary), 13)", blockchains=["ethereum","bnb","polygon","arbitrum","optimism","avalanche_c","gnosis","fantom","base"]),
                "clipperSwapTo": dict(samples["clipper"], selector='0xc4d652af', src_token_address="substr(cast(srcToken as varbinary), 13)", blockchains=["ethereum","bnb","polygon","arbitrum","optimism","avalanche_c","gnosis","fantom","base"], dst_receiver="recipient"),
                "ethUnoswap":    dict(samples["unoswap v6"], selector='0xa76dfc3b', src_token_address=native, src_token_amount="call_value", pools="array[dex]"),
                "ethUnoswap2":   dict(samples["unoswap v6"], selector='0x89af926a', src_token_address=native, src_token_amount="call_value", pools="array[dex,dex2]"),
                "ethUnoswap3":   dict(samples["unoswap v6"], selector='0x188ac35d', src_token_address=native, src_token_amount="call_value", pools="array[dex,dex2,dex3]"),
                "ethUnoswapTo":  dict(samples["unoswap v6"], selector='0x175accdc', src_token_address=native, src_token_amount="call_value", dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex]"),
                "ethUnoswapTo2": dict(samples["unoswap v6"], selector='0x0f449d71', src_token_address=native, src_token_amount="call_value", dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex,dex2]"),
                "ethUnoswapTo3": dict(samples["unoswap v6"], selector='0x493189f0', src_token_address=native, src_token_amount="call_value", dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex,dex2,dex3]"),
                "unoswap":       dict(samples["unoswap v6"], selector='0x83800a8e', pools="array[dex]"),
                "unoswap2":      dict(samples["unoswap v6"], selector='0x8770ba91', pools="array[dex,dex2]"),
                "unoswap3":      dict(samples["unoswap v6"], selector='0x19367472', pools="array[dex,dex2,dex3]"),
                "unoswapTo":     dict(samples["unoswap v6"], selector='0xe2c95c82', dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex]"),
                "unoswapTo2":    dict(samples["unoswap v6"], selector='0xea76dddf', dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex,dex2]"),
                "unoswapTo3":    dict(samples["unoswap v6"], selector='0xf7a70056', dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex,dex2,dex3]"),
                "permitAndCall": dict(auxiliary=true, selector='0x5816d723'),
            },
        },
    }
%}

{{ return(contracts) }}

{% endmacro %}