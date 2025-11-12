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
{% macro oneinch_cc_methods_cfg_macro(type="decoded") %}
    {% set immutables0 = oneinch_cc_immutables_cfg_macro(offset="0") %}
    {% set immutables1 = oneinch_cc_immutables_cfg_macro(offset="1") %}
    {% set immutables2 = oneinch_cc_immutables_cfg_macro(offset="2") %}
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
            "withdraw"      : dict(methodsV1.withdraw       , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "publicWithdraw": dict(methodsV1.publicWithdraw , flow="'src_withdraw'", secret="substr(call_input, 4 + 32*0 + 1, 32)"),
            "cancel"        : dict(methodsV1.cancel         , flow="'src_cancel'"),
            "rescueFunds"   : dict(methodsV1.rescueFunds    , flow="'src_rescue'"),
        }),
    }) }}
{% endmacro %}

-- LINEA CC CONFIG MACRO --
{% macro oneinch_linea_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- SONIC CC CONFIG MACRO --
{% macro oneinch_sonic_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}

-- UNICHAIN CC CONFIG MACRO --
{% macro oneinch_unichain_cc_contracts_cfg_macro() %} {{ return(oneinch_cc_contracts_cfg_macro()) }} {% endmacro %}