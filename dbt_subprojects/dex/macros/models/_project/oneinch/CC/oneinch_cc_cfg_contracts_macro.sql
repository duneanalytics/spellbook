{% macro oneinch_cc_cfg_contracts_macro() %}

-- CREATION IMMUTABLES SAMPLES --
{%
    set samples = {
        "v1": {
            "order_hash"    : "from_hex(creation_map['orderHash'])",
            "hashlock"      : "from_hex(creation_map['hashlock'])",
            "maker"         : "substr(cast(cast(creation_map['maker'] as uint256) as varbinary), 13)",
            "taker"         : "substr(cast(cast(creation_map['taker'] as uint256) as varbinary), 13)",
            "token"         : "substr(cast(cast(creation_map['token'] as uint256) as varbinary), 13)",
            "amount"        : "cast(creation_map['amount'] as uint256)",
            "safety_deposit": "cast(creation_map['safetyDeposit'] as uint256)",
            "timelocks"     : "cast(cast(creation_map['timelocks'] as uint256) as varbinary)",
            "factory"       : "cast(null as varbinary)",
            "escrow"        : "contract_address",
            "secret"        : "cast(null as varbinary)",
            "receiver"      : "cast(null as varbinary)",
            "nonce"         : "cast(null as varbinary)",
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
                "addressOfEscrowSrc": dict(samples["v1"], selector="0xfb6bd47e", flow="src_creation", nonce="0x02", factory="contract_address", escrow="output_0"),
                "createDstEscrow"   : dict(samples["v1"], selector="0xdea024e4", flow="dst_creation", nonce="0x03", factory="contract_address", escrow="cast(null as varbinary)", immutables="dstImmutables"),
            },
        },
        "EscrowSrcV1": {
            "version": "1",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "base", "zksync", "linea", "sonic", "unichain"],
            "start": "2024-08-20",
            "addresses": "creations",
            "methods": {
                "withdraw"      : dict(samples["v1"], selector="0x23305703", flow="src_withdraw", secret="secret"),
                "withdrawTo"    : dict(samples["v1"], selector="0x6c10c0c8", flow="src_withdraw", secret="secret", receiver="target"),
                "publicWithdraw": dict(samples["v1"], selector="0x0af97558", flow="src_withdraw", secret="secret"),
                "cancel"        : dict(samples["v1"], selector="0x90d3252f", flow="src_cancel"),
                "publicCancel"  : dict(samples["v1"], selector="0xdaff233e", flow="src_cancel"),
                "rescueFunds"   : dict(samples["v1"], selector="0x4649088b", flow="src_rescue"),
            },
        },
        "EscrowDstV1": {
            "version": "1",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "base", "zksync", "linea", "sonic", "unichain"],
            "start": "2024-08-20",
            "addresses": "creations",
            "methods": {
                "withdraw"      : dict(samples["v1"], selector="0x23305703", flow="dst_withdraw", secret="secret"),
                "publicWithdraw": dict(samples["v1"], selector="0x0af97558", flow="dst_withdraw", secret="secret"),
                "cancel"        : dict(samples["v1"], selector="0x90d3252f", flow="dst_cancel"),
                "rescueFunds"   : dict(samples["v1"], selector="0x4649088b", flow="dst_rescue"),
            },
        },
    }
%}

{{ return(contracts) }}

{% endmacro %}