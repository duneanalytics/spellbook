-- SAMPLES CONFIG --
{% macro oneinch_cc_immutables_cfg_macro(offset="0") %}
    {{ return({
        "v1": {
            "decoded": {
                "order_hash"    : "from_hex(json_value(data, 'lax $.orderHash'))",
                "hashlock"      : "from_hex(json_value(data, 'lax $.hashlock'))",
                "maker"         : "substr(cast(cast(json_extract_scalar(data, '$.maker') as uint256) as varbinary), 13)",
                "taker"         : "substr(cast(cast(json_extract_scalar(data, '$.taker') as uint256) as varbinary), 13)",
                "token"         : "substr(cast(cast(json_extract_scalar(data, '$.token') as uint256) as varbinary), 13)",
                "amount"        : "cast(json_extract_scalar(data, '$.amount') as uint256)",
                "safety_deposit": "cast(json_extract_scalar(data, '$.safetyDeposit') as uint256)",
                "timelocks"     : "cast(cast(json_extract_scalar(data, '$.timelocks') as uint256) as varbinary)",
                "escrow"        : "contract_address",
            },
            "raw": {
                "order_hash"    : "substr(call_input, 4 + 32*(0 + " + offset + ") + 1, 32)",
                "hashlock"      : "substr(call_input, 4 + 32*(1 + " + offset + ") + 1, 32)",
                "maker"         : "substr(call_input, 4 + 32*(2 + " + offset + ") + 12 + 1, 20)",
                "taker"         : "substr(call_input, 4 + 32*(3 + " + offset + ") + 12 + 1, 20)",
                "token"         : "substr(call_input, 4 + 32*(4 + " + offset + ") + 12 + 1, 20)",
                "amount"        : "bytearray_to_uint256(substr(call_input, 4 + 32*(5 + " + offset + ") + 1, 32))",
                "safety_deposit": "bytearray_to_uint256(substr(call_input, 4 + 32*(6 + " + offset + ") + 1, 32))",
                "timelocks"     : "substr(call_input, 4 + 32*(7 + " + offset + ") + 1, 32)",
                "escrow"        : "call_to",
            },
        },
    }) }}
{% endmacro %}

-- METHODS CONFIG --
-- v1_2: the v1.2 Immutables struct gained a trailing `bytes parameters`, which turns it into a dynamic tuple:
-- its calldata head becomes a pointer word and the fields land deeper (one extra word for methods where the
-- struct is the only preceding change; two for createDstEscrow, whose head is [pointer, srcCancellationTimestamp]).
-- Field extraction expressions are unchanged, so the v1 layouts are reused at shifted offsets. Selectors differ.
{% macro oneinch_cc_methods_cfg_macro(type="decoded") %}
    {% set immutables0 = oneinch_cc_immutables_cfg_macro(offset="0") %}
    {% set immutables1 = oneinch_cc_immutables_cfg_macro(offset="1") %}
    {% set immutables2 = oneinch_cc_immutables_cfg_macro(offset="2") %}
    {% set immutables3 = oneinch_cc_immutables_cfg_macro(offset="3") %}
    {{ return({
        "v1": {
            "addressOfEscrowSrc": dict(immutables0.v1[type], selector="0xfb6bd47e"),
            "createDstEscrow"   : dict(immutables0.v1[type], selector="0xdea024e4"),
            "withdraw"          : dict(immutables1.v1[type], selector="0x23305703"),
            "withdrawTo"        : dict(immutables2.v1[type], selector="0x6c10c0c8"),
            "publicWithdraw"    : dict(immutables1.v1[type], selector="0x0af97558"),
            "cancel"            : dict(immutables0.v1[type], selector="0x90d3252f"),
            "publicCancel"      : dict(immutables0.v1[type], selector="0xdaff233e"),
            "rescueFunds"       : dict(immutables2.v1[type], selector="0x4649088b"),
        },
        "v1_2": {
            "addressOfEscrowSrc": dict(immutables1.v1[type], selector="0xfd6de035"),
            "createDstEscrow"   : dict(immutables2.v1[type], selector="0xede88f38"),
            "withdraw"          : dict(immutables2.v1[type], selector="0x78a5e1a1"),
            "withdrawTo"        : dict(immutables3.v1[type], selector="0x4c345901"),
            "publicWithdraw"    : dict(immutables2.v1[type], selector="0x3e99ca75"),
            "cancel"            : dict(immutables1.v1[type], selector="0x2537a347"),
            "publicCancel"      : dict(immutables1.v1[type], selector="0x056fb477"),
            "rescueFunds"       : dict(immutables3.v1[type], selector="0x5e504d03"),
        },
    }) }}
{% endmacro %}

-- CONTRACTS CONFIG --
{% macro oneinch_cc_contracts_cfg_macro() %}
    {% set methodsV1 = oneinch_cc_methods_cfg_macro(type="decoded").v1 %}
    {{ return({
        "EscrowFactoryV1": {
            "version": "1",
            "start": "2024-08-20",
            "address": "0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a",
            "methods": {
                "addressOfEscrowSrc": dict(methodsV1.addressOfEscrowSrc , flow="'src_creation'", nonce="0x02", factory="contract_address", escrow="output_0"),
                "createDstEscrow"   : dict(methodsV1.createDstEscrow    , flow="'dst_creation'", nonce="0x03", factory="contract_address", escrow="cast(null as varbinary)", immutables="dstImmutables"),
            },
        },
        "EscrowSrcV1": {
            "version": "1",
            "start": "2024-08-20",
            "address": "creations",
            "initial_address": "0xcd70bf33cfe59759851db21c83ea47b6b83bef6a",
            "methods": {
                "withdraw"      : dict(methodsV1.withdraw       , flow="'src_withdraw'", secret="secret"),
                "withdrawTo"    : dict(methodsV1.withdrawTo     , flow="'src_withdraw'", secret="secret", receiver="target"),
                "publicWithdraw": dict(methodsV1.publicWithdraw , flow="'src_withdraw'", secret="secret"),
                "cancel"        : dict(methodsV1.cancel         , flow="'src_cancel'"),
                "publicCancel"  : dict(methodsV1.publicCancel   , flow="'src_cancel'"),
                "rescueFunds"   : dict(methodsV1.rescueFunds    , flow="'src_rescue'"),
            },
        },
        "EscrowDstV1": {
            "version": "1",
            "start": "2024-08-20",
            "address": "creations",
            "initial_address": "0x9c3e06659f1c34f930ce97fcbce6e04ae88e535b",
            "methods": {
                "withdraw"      : dict(methodsV1.withdraw       , flow="'dst_withdraw'", secret="secret"),
                "publicWithdraw": dict(methodsV1.publicWithdraw , flow="'dst_withdraw'", secret="secret"),
                "cancel"        : dict(methodsV1.cancel         , flow="'dst_cancel'"),
                "rescueFunds"   : dict(methodsV1.rescueFunds    , flow="'dst_rescue'"),
            },
        },
    }) }}
{% endmacro %}



-- ETHEREUM CC CONFIG MACRO --
{% macro oneinch_ethereum_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- BNB CC CONFIG MACRO --
{% macro oneinch_bnb_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- POLYGON CC CONFIG MACRO --
{% macro oneinch_polygon_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- ARBITRUM CC CONFIG MACRO --
{% macro oneinch_arbitrum_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- AVALANCHE CC CONFIG MACRO --
{% macro oneinch_avalanche_c_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- GNOSIS CC CONFIG MACRO --
{% macro oneinch_gnosis_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- OPTIMISM CC CONFIG MACRO --
{% macro oneinch_optimism_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- BASE CC CONFIG MACRO --
{% macro oneinch_base_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- ZKSYNC CC CONFIG MACRO --
-- zkSync Era is not EVM-equivalent, so Dune cannot decode the minimal-proxy EscrowSrc/Dst clones.
-- Their calldata is parsed from raw `call_input` (type="raw").
{% macro oneinch_zksync_cc_contracts_cfg_macro() %}
    {% set contracts = oneinch_cc_contracts_cfg_macro() %}
    {% set methodsV1 = oneinch_cc_methods_cfg_macro(type="raw").v1 %}
    {{ return({
        "EscrowFactoryV1": dict(contracts.EscrowFactoryV1, address="0x584aeab186d81dbb52a8a14820c573480c3d4773"),
        "EscrowSrcV1": dict(contracts.EscrowSrcV1, initial_address="0xddc60c7babfc55d8030f51910b157e179f7a41fc", methods={
            "withdraw"      : dict(methodsV1.withdraw       , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "withdrawTo"    : dict(methodsV1.withdrawTo     , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)", receiver="substr(call_input, 4 + 32*1 + 12 + 1, 20)"),
            "publicWithdraw": dict(methodsV1.publicWithdraw , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "cancel"        : dict(methodsV1.cancel         , flow="'src_cancel'"),
            "publicCancel"  : dict(methodsV1.publicCancel   , flow="'src_cancel'"),
            "rescueFunds"   : dict(methodsV1.rescueFunds    , flow="'src_rescue'"),
        }),
        "EscrowDstV1": dict(contracts.EscrowDstV1, initial_address="0xdc4ccc2fc2475d0ed3fddd563c44f2bf6a3900c9", methods={
            "withdraw"      : dict(methodsV1.withdraw       , flow="'dst_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "publicWithdraw": dict(methodsV1.publicWithdraw , flow="'dst_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "cancel"        : dict(methodsV1.cancel         , flow="'dst_cancel'"),
            "rescueFunds"   : dict(methodsV1.rescueFunds    , flow="'dst_rescue'"),
        }),
    }) }}
{% endmacro %}

-- LINEA CC CONFIG MACRO --
{% macro oneinch_linea_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- SONIC CC CONFIG MACRO --
{% macro oneinch_sonic_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- UNICHAIN CC CONFIG MACRO --
{% macro oneinch_unichain_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- ROBINHOOD CC CONFIG MACRO --
-- Robinhood has its own escrow factory + implementation deployments.
-- The Dune decoded EscrowSrcV1/EscrowDstV1 mappings on robinhood are contaminated: all escrow clones and both
-- implementations appear under both contract names, which mislabels src/dst flows and duplicates rows.
-- So escrow clones are resolved from creation traces and their calldata is parsed from raw `call_input`
-- (type="raw"), like on zksync. The factory (single healthy address) stays on decoded tables.
{% macro oneinch_robinhood_cc_contracts_cfg_macro() %}
    {% set contracts = oneinch_cc_contracts_cfg_macro() %}
    {% set methodsV1 = oneinch_cc_methods_cfg_macro(type="raw").v1 %}
    {{ return({
        "EscrowFactoryV1": dict(contracts.EscrowFactoryV1, address="0xa02b9cc95094bb27d1d041b9fbf09f65a366f7b3"),
        "EscrowSrcV1": dict(contracts.EscrowSrcV1, initial_address="0xb077a4326f1e875c21d74028a1499eafcee43bf3", methods={
            "withdraw"      : dict(methodsV1.withdraw       , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "withdrawTo"    : dict(methodsV1.withdrawTo     , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)", receiver="substr(call_input, 4 + 32*1 + 12 + 1, 20)"),
            "publicWithdraw": dict(methodsV1.publicWithdraw , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "cancel"        : dict(methodsV1.cancel         , flow="'src_cancel'"),
            "publicCancel"  : dict(methodsV1.publicCancel   , flow="'src_cancel'"),
            "rescueFunds"   : dict(methodsV1.rescueFunds    , flow="'src_rescue'"),
        }),
        "EscrowDstV1": dict(contracts.EscrowDstV1, initial_address="0x104f09ea1f9c09662635ad581d0bef8b15d16f4f", methods={
            "withdraw"      : dict(methodsV1.withdraw       , flow="'dst_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "publicWithdraw": dict(methodsV1.publicWithdraw , flow="'dst_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "cancel"        : dict(methodsV1.cancel         , flow="'dst_cancel'"),
            "rescueFunds"   : dict(methodsV1.rescueFunds    , flow="'dst_rescue'"),
        }),
    }) }}
{% endmacro %}

-- SHARED CROSS-CHAIN V1.2 CONFIG (august 2026 deployment wave: cronos, monad, hyperevm) --
-- These chains run the cross-chain v1.2 contracts: same factory/implementation addresses on all three.
-- The factory decode is healthy, so factory methods stay on decoded tables. The EscrowSrc/Dst minimal-proxy
-- clones are not decoded on Dune, so, like on zksync and robinhood, clones are resolved from creation traces
-- and their calldata is parsed from raw call_input (type="raw").
-- A v1.0-ABI factory (0x9e010857ed5aaa4fca6d5404f7c7c54b1bbb8ad2) is also deployed on these chains but has no
-- activity; it is intentionally not configured here. Revisit if it ever becomes active.
-- addressOfEscrowSrc is not called on-chain by the v1.2 resolvers (the escrow address is computed off-chain),
-- so src_creation rows are not expected; src escrow withdraw/cancel flows are still captured through the
-- creations-based clone resolution.
-- v1.2 dst escrow address: the CREATE2 salt is keccak over the 8 static immutables fields (timelocks patched
-- with the deploy block time in the top 4 bytes) plus keccak(parameters) as a 9th word. The built-in fallback
-- in oneinch_cc_macro implements the v1.0/v1.1 salt (8 fields, no parameters hash), so the full expression is
-- passed here instead. Validated against all on-chain v1.2 dst escrow deployments on the three chains.
{% macro oneinch_cc_v1_2_raw_contracts_cfg_macro() %}
    {% set methodsV1_2_decoded = oneinch_cc_methods_cfg_macro(type="decoded").v1_2 %}
    {% set methodsV1_2 = oneinch_cc_methods_cfg_macro(type="raw").v1_2 %}
    {% set params_off = "cast(bytearray_to_bigint(substr(call_input, 4 + 32*10 + 25, 8)) as int)" %}
    {% set params_len = "cast(bytearray_to_bigint(substr(call_input, 4 + 32*2 + " ~ params_off ~ " + 25, 8)) as int)" %}
    {% set params = "substr(call_input, 4 + 32*2 + " ~ params_off ~ " + 32 + 1, " ~ params_len ~ ")" %}
    {% set dst_salt = "keccak(concat(substr(call_input, 4 + 32*2 + 1, 32*7), to_big_endian_32(cast(to_unixtime(block_time) as int)), substr(call_input, 4 + 32*9 + 4 + 1, 28), keccak(" ~ params ~ ")))" %}
    {% set dst_escrow = "substr(keccak(concat(0xff, call_to, " ~ dst_salt ~ ", keccak(concat(0x3d602d80600a3d3981f3363d3d373d3d3d363d73, 0x715bb4091abccacef523a6069b4f0e2678a352f2, 0x5af43d82803e903d91602b57fd5bf3)))), 13)" %}
    {{ return({
        "EscrowFactoryV1_2": {
            "version": "1.2",
            "start": "2026-08-01",
            "address": "0x8e6c3c2e2631de0a1d4fd46a15f79a1373486fa4",
            "methods": {
                "addressOfEscrowSrc": dict(methodsV1_2_decoded.addressOfEscrowSrc , flow="'src_creation'", nonce="0x01", factory="contract_address", escrow="output_0"),
                "createDstEscrow"   : dict(methodsV1_2_decoded.createDstEscrow    , flow="'dst_creation'", nonce="0x02", factory="contract_address", escrow=dst_escrow, immutables="dstImmutables"),
            },
        },
        "EscrowSrcV1_2": {
            "version": "1.2",
            "start": "2026-08-01",
            "address": "creations",
            "initial_address": "0x25fb2e7a56db5a3f04be5e7a728977e889e62a3c",
            "methods": {
                "withdraw"      : dict(methodsV1_2.withdraw       , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
                "withdrawTo"    : dict(methodsV1_2.withdrawTo     , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)", receiver="substr(call_input, 4 + 32*1 + 12 + 1, 20)"),
                "publicWithdraw": dict(methodsV1_2.publicWithdraw , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
                "cancel"        : dict(methodsV1_2.cancel         , flow="'src_cancel'"),
                "publicCancel"  : dict(methodsV1_2.publicCancel   , flow="'src_cancel'"),
                "rescueFunds"   : dict(methodsV1_2.rescueFunds    , flow="'src_rescue'"),
            },
        },
        "EscrowDstV1_2": {
            "version": "1.2",
            "start": "2026-08-01",
            "address": "creations",
            "initial_address": "0x715bb4091abccacef523a6069b4f0e2678a352f2",
            "methods": {
                "withdraw"      : dict(methodsV1_2.withdraw       , flow="'dst_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
                "publicWithdraw": dict(methodsV1_2.publicWithdraw , flow="'dst_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
                "cancel"        : dict(methodsV1_2.cancel         , flow="'dst_cancel'"),
                "rescueFunds"   : dict(methodsV1_2.rescueFunds    , flow="'dst_rescue'"),
            },
        },
    }) }}
{% endmacro %}

-- CRONOS CC CONFIG MACRO --
{% macro oneinch_cronos_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_v1_2_raw_contracts_cfg_macro()) }} {% endmacro %}

-- MONAD CC CONFIG MACRO --
{% macro oneinch_monad_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_v1_2_raw_contracts_cfg_macro()) }} {% endmacro %}

-- HYPEREVM CC CONFIG MACRO --
{% macro oneinch_hyperevm_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_v1_2_raw_contracts_cfg_macro()) }} {% endmacro %}