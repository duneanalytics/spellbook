{% macro oneinch_escrow_cfg_factories_macro() %}

-- CREATION IMMUTABLES SAMPLES
{%
    set immutables = {
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

-- CONTRACTS CONFIG
{%
    set contracts = {
        "EscrowFactoryV1": {
            "version": "1",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "base", "zksync", "linea", "sonic", "unichain"],
            "start": "2024-08-20",
            "dst_creation": dict(immutables["v1"], method="createDstEscrow"),
            "src_created":  dict(immutables["v1"], event="SrcEscrowCreated",
                dst_maker="substr(cast(cast(complement_map['maker'] as uint256) as varbinary), 13)",
                dst_token="substr(cast(cast(complement_map['token'] as uint256) as varbinary), 13)",
                dst_amount="cast(complement_map['amount'] as uint256)",
                dst_chain_id="cast(complement_map['chainId'] as uint256)",
            ),
        },
    }
%}

{{ return(contracts) }}

{% endmacro %}