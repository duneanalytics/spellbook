{% macro oneinch_cc_cfg_contracts_macro() %}

-- CREATION IMMUTABLES SAMPLES --
{%
    set samples = {
        "v1": {
            "order_hash":       "from_hex(creation_map['orderHash'])",
            "hashlock":         "from_hex(creation_map['hashlock'])",
            "maker":            "substr(cast(cast(creation_map['maker'] as uint256) as varbinary), 13)",
            "taker":            "substr(cast(cast(creation_map['taker'] as uint256) as varbinary), 13)",
            "token":            "substr(cast(cast(creation_map['token'] as uint256) as varbinary), 13)",
            "amount":           "cast(creation_map['amount'] as uint256)",
            "safety_deposit":   "cast(creation_map['safetyDeposit'] as uint256)",
            "timelocks":        "cast(cast(creation_map['timelocks'] as uint256) as varbinary)",
        },
    }
%}

-- CONTRACTS CONFIG --
{%
    set contracts = {
        "EscrowFactoryV1": {
            "version": "1",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "base", "zksync", "linea", "sonic", "unichain"],
            "start": "2024-08-20",
            "addresses": {
                "0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a": ['ethereum', 'bnb', 'polygon', 'arbitrum', 'avalanche_c', 'gnosis', 'optimism', 'base', 'linea', 'sonic', 'unichain'],
                "0x584aeab186d81dbb52a8a14820c573480c3d4773": ['zksync'],
            },
            "methods": {
                "createDstEscrow": dict(samples["v1"], selector="0xdea024e4"),
            },
            "events": {
                "SrcEscrowCreated": dict(samples["v1"], topic0="0x0e534c62f0afd2fa0f0fa71198e8aa2d549f24daf2bb47de0d5486c7ce9288ca",
                    dst_maker="substr(cast(cast(complement_map['maker'] as uint256) as varbinary), 13)",
                    dst_token="substr(cast(cast(complement_map['token'] as uint256) as varbinary), 13)",
                    dst_amount="cast(complement_map['amount'] as uint256)",
                    dst_chain_id="cast(complement_map['chainId'] as uint256)",
                ),
            },
        },
        "EscrowV1": {
            "version": "1",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "base", "zksync", "linea", "sonic", "unichain"],
            "start": "2024-08-20",
            "addresses": "creations",
            "methods": {
                "withdraw": dict(selector="0x23305703",
                    secret          ="substr(input, 4 + 32*0 + 1, 32)",
                    order_hash      ="substr(input, 4 + 32*1 + 1, 32)",
                    hashlock        ="substr(input, 4 + 32*2 + 1, 32)",
                    maker           ="substr(input, 4 + 32*3 + 12 + 1, 20)",
                    receiver        ="substr(input, 4 + 32*4 + 12 + 1, 20)",
                    token           ="substr(input, 4 + 32*5 + 12 + 1, 20)",
                    amount          ="bytearray_to_uint256(substr(input, 4 + 32*6 + 1, 32))",
                    safety_deposit  ="bytearray_to_uint256(substr(input, 4 + 32*7 + 1, 32))",
                    timelocks       ="substr(input, 4 + 32*8 + 1, 32)",
                ),
                "cancel": dict(selector="0x90d3252f",
                    order_hash      ="substr(input, 4 + 32*0 + 1, 32)",
                    hashlock        ="substr(input, 4 + 32*1 + 1, 32)",
                    receiver        ="substr(input, 4 + 32*2 + 12 + 1, 20)",
                    taker           ="substr(input, 4 + 32*3 + 12 + 1, 20)",
                    token           ="substr(input, 4 + 32*4 + 12 + 1, 20)",
                    amount          ="bytearray_to_uint256(substr(input, 4 + 32*5 + 1, 32))",
                    safety_deposit  ="bytearray_to_uint256(substr(input, 4 + 32*6 + 1, 32))",
                    timelocks       ="substr(input, 4 + 32*7 + 1, 32)",
                ),
                "rescueFunds": dict(selector="0x4649088b",
                    rescue_token    ="substr(input, 4 + 32*0 + 12 + 1, 20)",
                    rescue_amount   ="bytearray_to_uint256(substr(input, 4 + 32*1 + 1, 32))",
                    order_hash      ="substr(input, 4 + 32*2 + 1, 32)",
                    hashlock        ="substr(input, 4 + 32*3 + 1, 32)",
                    maker           ="substr(input, 4 + 32*4 + 12 + 1, 20)",
                    receiver        ="substr(input, 4 + 32*5 + 12 + 1, 20)",
                    token           ="substr(input, 4 + 32*6 + 12 + 1, 20)",
                    amount          ="bytearray_to_uint256(substr(input, 4 + 32*7 + 1, 32))",
                    safety_deposit  ="bytearray_to_uint256(substr(input, 4 + 32*8 + 1, 32))",
                    timelocks       ="substr(input, 4 + 32*9 + 1, 32)",
                ),
            },
        },
    }
%}

{{ return(contracts) }}

{% endmacro %}