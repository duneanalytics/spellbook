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

-- SHARED CROSS-CHAIN V1.2 CONTRACTS CONFIG --
-- One order-schema-v1.2 deployment = (factory, src impl, dst impl); the impl addresses differ per deployment wave:
--   canonical 11 chains (2026-02-04): 0x03a25b32... / 0x30476b0b... / 0x5cd822f1...
--   cronos / monad / hyperevm (2026-08): 0x8e6c3c2e... / 0x25fb2e7a... / 0x715bb409...
--   robinhood (2026-07-15): 0x50d26ea1... / 0xb540a42c... / 0x3875faf1...
--   zksync (2026-02-06): 0xd9085ac0... / 0x198cc9a0... / 0x07d3d5e5...
-- The factory legs read the EscrowFactoryV1_2_call_* decoded tables and the escrow legs read the per-chain
-- EscrowSrcV1_2/EscrowDstV1_2 decoded clone tables (clone-anchored groups on Dune).
-- escrow_type="raw" is for zksync only: Dune cannot decode the zkEVM minimal-proxy clones, so its escrow
-- legs resolve clones from creation traces (parent_code_offset) and parse calldata from raw call_input.
-- v1.2 dst escrow address: the CREATE2 salt is keccak over the 8 static immutables fields (timelocks patched
-- with the deploy block time in the top 4 bytes) plus keccak(parameters) as a 9th word, and the proxy hash
-- embeds the dst impl (plain EIP-1167). The built-in fallback in oneinch_cc_macro implements the v1.0/v1.1
-- salt (8 fields, nonce-derived impl), so the full expression is passed here instead. Validated against all
-- on-chain v1.2 dst escrow deployments on cronos / monad / hyperevm.
-- NOTE: on zksync the CREATE2 math does not hold (zkEVM address derivation), so dst_creation `escrow` values
-- are wrong there — same known limitation as the v1 line on zksync; executions are unaffected (escrow = call_to).
{% macro oneinch_cc_v1_2_contracts_cfg_macro(factory, src_impl, dst_impl, start, escrow_type="decoded", parent_code_offset=none) %}
    {% set methods_factory = oneinch_cc_methods_cfg_macro(type="decoded").v1_2 %}
    {% set methods = oneinch_cc_methods_cfg_macro(type=escrow_type).v1_2 %}
    {% set params_off = "cast(bytearray_to_bigint(substr(call_input, 4 + 32*10 + 25, 8)) as int)" %}
    {% set params_len = "cast(bytearray_to_bigint(substr(call_input, 4 + 32*2 + " ~ params_off ~ " + 25, 8)) as int)" %}
    {% set params = "substr(call_input, 4 + 32*2 + " ~ params_off ~ " + 32 + 1, " ~ params_len ~ ")" %}
    {% set dst_salt = "keccak(concat(substr(call_input, 4 + 32*2 + 1, 32*7), to_big_endian_32(cast(to_unixtime(block_time) as int)), substr(call_input, 4 + 32*9 + 4 + 1, 28), keccak(" ~ params ~ ")))" %}
    {% set dst_escrow = "substr(keccak(concat(0xff, call_to, " ~ dst_salt ~ ", keccak(concat(0x3d602d80600a3d3981f3363d3d373d3d3d363d73, " ~ dst_impl ~ ", 0x5af43d82803e903d91602b57fd5bf3)))), 13)" %}
    {% if escrow_type == "raw" %}
        {% set secret = "substr(call_input, 4 + 32*0 + 1, 32)" %}
        {% set receiver = "substr(call_input, 4 + 32*1 + 12 + 1, 20)" %}
    {% else %}
        {% set secret = "secret" %}
        {% set receiver = "target" %}
    {% endif %}
    {% set contracts = {
        "EscrowFactoryV1_2": {
            "version": "1.2",
            "start": start,
            "address": factory,
            "methods": {
                "addressOfEscrowSrc": dict(methods_factory.addressOfEscrowSrc , flow="'src_creation'", nonce="0x01", factory="contract_address", escrow="output_0"),
                "createDstEscrow"   : dict(methods_factory.createDstEscrow    , flow="'dst_creation'", nonce="0x02", factory="contract_address", escrow=dst_escrow, immutables="dstImmutables"),
            },
        },
        "EscrowSrcV1_2": {
            "version": "1.2",
            "start": start,
            "address": "creations",
            "initial_address": src_impl,
            "methods": {
                "withdraw"      : dict(methods.withdraw       , flow="'src_withdraw'", secret=secret),
                "withdrawTo"    : dict(methods.withdrawTo     , flow="'src_withdraw'", secret=secret, receiver=receiver),
                "publicWithdraw": dict(methods.publicWithdraw , flow="'src_withdraw'", secret=secret),
                "cancel"        : dict(methods.cancel         , flow="'src_cancel'"),
                "publicCancel"  : dict(methods.publicCancel   , flow="'src_cancel'"),
                "rescueFunds"   : dict(methods.rescueFunds    , flow="'src_rescue'"),
            },
        },
        "EscrowDstV1_2": {
            "version": "1.2",
            "start": start,
            "address": "creations",
            "initial_address": dst_impl,
            "methods": {
                "withdraw"      : dict(methods.withdraw       , flow="'dst_withdraw'", secret=secret),
                "publicWithdraw": dict(methods.publicWithdraw , flow="'dst_withdraw'", secret=secret),
                "cancel"        : dict(methods.cancel         , flow="'dst_cancel'"),
                "rescueFunds"   : dict(methods.rescueFunds    , flow="'dst_rescue'"),
            },
        },
    } %}
    {% if parent_code_offset is not none %}
        {% do contracts.EscrowSrcV1_2.update({"creations_parent_code_offset": parent_code_offset}) %}
        {% do contracts.EscrowDstV1_2.update({"creations_parent_code_offset": parent_code_offset}) %}
    {% endif %}
    {{ return(contracts) }}
{% endmacro %}

-- CONTRACTS CONFIG --
-- The base config carries both generations for the canonical 11 chains, fully on decoded tables: the v1
-- line (healthy since the Oct 2025 cleanup) and the v1.2 line (live since 2026-02-04). Where a per-chain
-- EscrowSrcV1_2/EscrowDstV1_2 decoding is still backfilling on Dune the corresponding leg is simply empty
-- until a full refresh after the backfill lands.
{% macro oneinch_cc_contracts_cfg_macro() %}
    {% set methodsV1 = oneinch_cc_methods_cfg_macro(type="decoded").v1 %}
    {% set contracts = {
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
    } %}
    {% do contracts.update(oneinch_cc_v1_2_contracts_cfg_macro(
        factory="0x03a25b3215a0e5c15cf23ac4d2e5cf86c0ff7efa",
        src_impl="0x30476b0bf2f73f78a75488742920da7ff76c0ca0",
        dst_impl="0x5cd822f1c70469c36898aa98516a48f4aa04c73a",
        start="2026-02-04",
    )) %}
    {{ return(contracts) }}
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
-- Their calldata is parsed from raw `call_input` (type="raw"). Both factory generations decode fine.
-- Clone-code offsets: since the ~2026-04 zksync EVM-emulation migration, creation_traces.code for the
-- v1 zk-format clones is a 128-byte blob with the impl address right-aligned in the last word (byte 109);
-- the old byte-13 layout is gone from the re-backfilled table, which silently froze the prod v1 escrow
-- legs between 2026-04-10 and this fix (recovered by full refresh). The v1.2 factory emits EVM-format
-- 160-byte clones with the impl at byte 11. Hence the per-contract offset overrides.
{% macro oneinch_zksync_cc_contracts_cfg_macro() %}
    {% set base = oneinch_cc_contracts_cfg_macro() %}
    {% set methodsV1 = oneinch_cc_methods_cfg_macro(type="raw").v1 %}
    {% set contracts = {
        "EscrowFactoryV1": dict(base.EscrowFactoryV1, address="0x584aeab186d81dbb52a8a14820c573480c3d4773"),
        "EscrowSrcV1": dict(base.EscrowSrcV1, initial_address="0xddc60c7babfc55d8030f51910b157e179f7a41fc", creations_parent_code_offset=109, methods={
            "withdraw"      : dict(methodsV1.withdraw       , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "withdrawTo"    : dict(methodsV1.withdrawTo     , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)", receiver="substr(call_input, 4 + 32*1 + 12 + 1, 20)"),
            "publicWithdraw": dict(methodsV1.publicWithdraw , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "cancel"        : dict(methodsV1.cancel         , flow="'src_cancel'"),
            "publicCancel"  : dict(methodsV1.publicCancel   , flow="'src_cancel'"),
            "rescueFunds"   : dict(methodsV1.rescueFunds    , flow="'src_rescue'"),
        }),
        "EscrowDstV1": dict(base.EscrowDstV1, initial_address="0xdc4ccc2fc2475d0ed3fddd563c44f2bf6a3900c9", creations_parent_code_offset=109, methods={
            "withdraw"      : dict(methodsV1.withdraw       , flow="'dst_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "publicWithdraw": dict(methodsV1.publicWithdraw , flow="'dst_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "cancel"        : dict(methodsV1.cancel         , flow="'dst_cancel'"),
            "rescueFunds"   : dict(methodsV1.rescueFunds    , flow="'dst_rescue'"),
        }),
    } %}
    {% do contracts.update(oneinch_cc_v1_2_contracts_cfg_macro(
        factory="0xd9085ac07da21bd6eb003a530a524ab054ca8652",
        src_impl="0x198cc9a03192d767a29886b6ef626fee38e36959",
        dst_impl="0x07d3d5e598cc23bfee9884b1e342ddaecd88dead",
        start="2026-02-06",
        escrow_type="raw",
        parent_code_offset=11,
    )) %}
    {{ return(contracts) }}
{% endmacro %}

-- LINEA CC CONFIG MACRO --
{% macro oneinch_linea_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- SONIC CC CONFIG MACRO --
{% macro oneinch_sonic_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- UNICHAIN CC CONFIG MACRO --
{% macro oneinch_unichain_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- ROBINHOOD CC CONFIG MACRO --
-- Robinhood has its own escrow deployments for both generations: the v1-ABI line (since 2026-06-01, live
-- minority) and the v1.2 line (since 2026-07-15, dominant). Both run fully on decoded tables, same as the
-- canonical chains. The escrow legs fill in as Dune finishes re-anchoring the robinhood escrow groups on
-- clones (DF-1176) and backfills them; until then only the factory legs produce rows.
{% macro oneinch_robinhood_cc_contracts_cfg_macro() %}
    {% set base = oneinch_cc_contracts_cfg_macro() %}
    {% set contracts = {
        "EscrowFactoryV1": dict(base.EscrowFactoryV1, start="2026-06-01", address="0xa02b9cc95094bb27d1d041b9fbf09f65a366f7b3"),
        "EscrowSrcV1"    : dict(base.EscrowSrcV1    , start="2026-06-01", initial_address="0xb077a4326f1e875c21d74028a1499eafcee43bf3"),
        "EscrowDstV1"    : dict(base.EscrowDstV1    , start="2026-06-01", initial_address="0x104f09ea1f9c09662635ad581d0bef8b15d16f4f"),
    } %}
    {% do contracts.update(oneinch_cc_v1_2_contracts_cfg_macro(
        factory="0x50d26ea1e2460b3a42ff47466b955fc6bd906013",
        src_impl="0xb540a42ca356f30307439b60647b5704d57f45ee",
        dst_impl="0x3875faf11ccef1dca35d190bc41cd47895dc18b2",
        start="2026-07-15",
    )) %}
    {{ return(contracts) }}
{% endmacro %}

-- CRONOS / MONAD / HYPEREVM / ARC CC CONFIG MACROS --
-- The august 2026 deployment wave (arc joined 2026-09) shares the same addresses on all four chains, both
-- generations, fully on
-- decoded tables (all clone-anchored on Dune since 2026-09-14):
--   v1.2 line (protocol "v1.1", the active one): factory 0x8e6c3c2e..., impls 0x25fb2e7a... / 0x715bb409...
--   v1-ABI line (protocol "legacy v1.0"): factory 0x9e010857..., impls src 0x57f60782... / dst 0x7b1bfa2a...
-- The v1-ABI factory (deployed 2026-08-04, first traffic 2026-08-21) only ever receives dst escrows (fills of
-- orders sourced on other chains): zero src clones exist on-chain, so there is no EscrowSrcV1 decoding and no
-- EscrowSrcV1 entry here — add one if a src clone ever appears (and gets decoded). Its impls sit at factory
-- nonces 2 / 3 like the canonical v1 deployment, so the base EscrowFactoryV1 / EscrowDstV1 entries are reused
-- with the addresses swapped (the createDstEscrow CREATE2 fallback in oneinch_cc_macro depends on the nonce).
-- addressOfEscrowSrc is rarely called on-chain by the v1.2 resolvers here (the escrow address is computed
-- off-chain), so src_creation rows are sparse; src escrow withdraw/cancel flows are still captured through
-- the decoded clone call tables.
{% macro oneinch_cc_new_chains_contracts_cfg_macro() %}
    {% set base = oneinch_cc_contracts_cfg_macro() %}
    {% set contracts = {
        "EscrowFactoryV1": dict(base.EscrowFactoryV1, start="2026-08-04", address="0x9e010857ed5aaa4fca6d5404f7c7c54b1bbb8ad2"),
        "EscrowDstV1"    : dict(base.EscrowDstV1    , start="2026-08-04", initial_address="0x7b1bfa2a3227b1dbfc886da843b527bd3e79864a"),
    } %}
    {% do contracts.update(oneinch_cc_v1_2_contracts_cfg_macro(
        factory="0x8e6c3c2e2631de0a1d4fd46a15f79a1373486fa4",
        src_impl="0x25fb2e7a56db5a3f04be5e7a728977e889e62a3c",
        dst_impl="0x715bb4091abccacef523a6069b4f0e2678a352f2",
        start="2026-08-01",
    )) %}
    {{ return(contracts) }}
{% endmacro %}

-- CRONOS CC CONFIG MACRO --
{% macro oneinch_cronos_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_new_chains_contracts_cfg_macro()) }} {% endmacro %}

-- MONAD CC CONFIG MACRO --
{% macro oneinch_monad_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_new_chains_contracts_cfg_macro()) }} {% endmacro %}

-- HYPEREVM CC CONFIG MACRO --
{% macro oneinch_hyperevm_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_new_chains_contracts_cfg_macro()) }} {% endmacro %}

-- ARC CC CONFIG MACRO --
{% macro oneinch_arc_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_new_chains_contracts_cfg_macro()) }} {% endmacro %}
