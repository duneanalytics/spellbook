-- STREAMS CONFIGURATIONS --

-- start dates for sreams by default:
-- for ar: 2019-06-01
-- for lo: 2021-06-01 / fusion: 2022-12-22
-- for cc: 2024-08-20
-- for a quick CI, change the start dates of the streams to light/easy

{% macro oneinch_ar_cfg_macro() %} {{ return({"name": "ar", "start": "2019-06-01", "mode": "'classic'"}) }} {% endmacro %}
{% macro oneinch_lo_cfg_macro() %} {{ return({"name": "lo", "start": "2021-06-01", "mode": "if(flags['fusion'], 'fusion', 'limits')"}) }} {% endmacro %}
{% macro oneinch_cc_cfg_macro() %} {{ return({"name": "cc", "start": "2024-08-20", "mode": "'cross-chain'"}) }} {% endmacro %}

{% macro oneinch_streams_cfg_macro() %}
    {{ return([
        oneinch_ar_cfg_macro(),
        oneinch_lo_cfg_macro(),
        oneinch_cc_cfg_macro(),
    ]) }}
{% endmacro %}

-- SUBSTREAMS CONFIGURATIONS --
{% macro oneinch_ar_raw_calls_cfg_macro()   %} {{ return(dict(oneinch_ar_cfg_macro(), start="2019-06-01")) }} {% endmacro %}
{% macro oneinch_ar_transfers_cfg_macro()   %} {{ return(dict(oneinch_ar_cfg_macro(), start="2019-06-01")) }} {% endmacro %}
{% macro oneinch_ar_executions_cfg_macro()  %} {{ return(dict(oneinch_ar_cfg_macro(), start="2019-06-01")) }} {% endmacro %}

{% macro oneinch_lo_raw_calls_cfg_macro()   %} {{ return(dict(oneinch_lo_cfg_macro(), start="2021-06-01")) }} {% endmacro %}
{% macro oneinch_lo_transfers_cfg_macro()   %} {{ return(dict(oneinch_lo_cfg_macro(), start="2021-06-01")) }} {% endmacro %}
{% macro oneinch_lo_executions_cfg_macro()  %} {{ return(dict(oneinch_lo_cfg_macro(), start="2021-06-01")) }} {% endmacro %}
{% macro oneinch_lo_fusion_cfg_macro()      %} {{ return(dict(oneinch_lo_cfg_macro(), start="2022-12-22")) }} {% endmacro %}

{% macro oneinch_cc_raw_calls_cfg_macro()   %} {{ return(dict(oneinch_cc_cfg_macro(), start="2024-08-20")) }} {% endmacro %}
{% macro oneinch_cc_transfers_cfg_macro()   %} {{ return(dict(oneinch_cc_cfg_macro(), start="2024-08-20")) }} {% endmacro %}
{% macro oneinch_cc_executions_cfg_macro()  %} {{ return(dict(oneinch_cc_cfg_macro(), start="2024-08-20")) }} {% endmacro %}



-- CONTRACTS CONFIGURATIONS --
{% macro oneinch_meta_contracts_cfg_macro() %}
    {{ return({
        "AccessTokenLimitsV1": {
            "version"   : "1",
            "type"      : "AccessToken",
            "mode"      : "limits",
            "address"   : "0xacce5500000f71a32b5e5514d1577e14b7aacc4a",
            "start"     : "2024-08-28",
        },
        "AccessTokenFusionV1": {
            "version"   : "1",
            "type"      : "AccessToken",
            "mode"      : "fusion",
            "address"   : "0xacce550000863572b867e661647cd7d97b72c507",
            "start"     : "2024-08-28",
        },
        "AccessTokenCrossChainV1": {
            "version"   : "1",
            "type"      : "AccessToken",
            "mode"      : "cross-chain",
            "address"   : "0xacce550000159e70908c0499a1119d04e7039c28",
            "start"     : "2024-08-28",
        },
    }) }}
{% endmacro %}



-- BLOCKCHAINS CONFIGURATIONS --

{% macro oneinch_ethereum_cfg_macro() %}
    {{ return({
        "name"                          : "ethereum",
        "start"                         : "2019-06-03",
        "chain_id"                      : "1",
        "native_token_symbol"           : "'ETH'",
        "wrapped_native_token_address"  : "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "explorer_link"                 : "'https://etherscan.io'",
        "fusion_settlement_addresses"   : ['0x399740157391a9f1bf4e9921a8834f9bc8f2678e', '0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0xa88800cd213da5ae406ce248380802bd53b47647'],
        "escrow_factory_addresses"      : ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
        "atokens"                       : true,
    }) }}
{% endmacro %}

{% macro oneinch_bnb_cfg_macro() %}
    {{ return({
        "name"                          : "bnb",
        "start"                         : "2021-02-18",
        "chain_id"                      : "56",
        "native_token_symbol"           : "'BNB'",
        "wrapped_native_token_address"  : "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
        "explorer_link"                 : "'https://bscscan.com'",
        "fusion_settlement_addresses"   : ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0x1d0ae300eec4093cee4367c00b228d10a5c7ac63'],
        "escrow_factory_addresses"      : ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
        "atokens"                       : false,
    }) }}
{% endmacro %}

{% macro oneinch_polygon_cfg_macro() %}
    {{ return({
        "name"                          : "polygon",
        "start"                         : "2021-05-05",
        "chain_id"                      : "137",
        "native_token_symbol"           : "'MATIC'",
        "wrapped_native_token_address"  : "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
        "explorer_link"                 : "'https://polygonscan.com'",
        "fusion_settlement_addresses"   : ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0x1e8ae092651e7b14e4d0f93611267c5be19b8b9f'],
        "escrow_factory_addresses"      : ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
        "atokens"                       : true,
    }) }}
{% endmacro %}

{% macro oneinch_arbitrum_cfg_macro() %}
    {{ return({
        "name"                          : "arbitrum",
        "start"                         : "2021-06-22",
        "chain_id"                      : "42161",
        "native_token_symbol"           : "'ETH'",
        "wrapped_native_token_address"  : "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        "explorer_link"                 : "'https://arbiscan.io'",
        "fusion_settlement_addresses"   : ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0x4bc3e539aaa5b18a82f6cd88dc9ab0e113c63377'],
        "escrow_factory_addresses"      : ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a', '0xc02e6487fbf69d6849b4b9ad9ec0bf5ff8d0c2a1'],
        "atokens"                       : true,
    }) }}
{% endmacro %}

{% macro oneinch_optimism_cfg_macro() %}
    {{ return({
        "name"                          : "optimism",
        "start"                         : "2021-11-12",
        "chain_id"                      : "10",
        "native_token_symbol"           : "'ETH'",
        "wrapped_native_token_address"  : "0x4200000000000000000000000000000000000006",
        "explorer_link"                 : "'https://optimistic.etherscan.io'",
        "fusion_settlement_addresses"   : ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0xd89adc20c400b6c45086a7f6ab2dca19745b89c2'],
        "escrow_factory_addresses"      : ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
        "atokens"                       : true,
    }) }}
{% endmacro %}

{% macro oneinch_avalanche_c_cfg_macro() %}
    {{ return({
        "name"                          : "avalanche_c",
        "start"                         : "2021-12-22",
        "chain_id"                      : "43114",
        "native_token_symbol"           : "'AVAX'",
        "wrapped_native_token_address"  : "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
        "explorer_link"                 : "'https://snowtrace.io'",
        "fusion_settlement_addresses"   : ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0x7731f8df999a9441ae10519617c24568dc82f697'],
        "escrow_factory_addresses"      : ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
        "atokens"                       : true,
    }) }}
{% endmacro %}

{% macro oneinch_gnosis_cfg_macro() %}
    {{ return({
        "name"                          : "gnosis",
        "start"                         : "2021-12-22",
        "chain_id"                      : "100",
        "native_token_symbol"           : "'xDAI'",
        "wrapped_native_token_address"  : "0xe91d153e0b41518a2ce8dd3d7944fa863463a97d",
        "explorer_link"                 : "'https://gnosisscan.io'",
        "fusion_settlement_addresses"   : ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0xcbdb7490968d4dbf183c60fc899c2e9fbd445308'],
        "escrow_factory_addresses"      : ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
        "atokens"                       : true,
    }) }}
{% endmacro %}

{% macro oneinch_fantom_cfg_macro() %}
    {{ return({
        "name"                          : "fantom",
        "start"                         : "2022-03-16",
        "chain_id"                      : "250",
        "native_token_symbol"           : "'FTM'",
        "wrapped_native_token_address"  : "0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83",
        "explorer_link"                 : "'https://ftmscan.com'",
        "fusion_settlement_addresses"   : ['0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0xa218543cc21ee9388fa1e509f950fd127ca82155'],
        "escrow_factory_addresses"      : ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
        "atokens"                       : false,
    }) }}
{% endmacro %}

{% macro oneinch_aurora_cfg_macro() %}
    {{ return({
        "name"                          : "aurora",
        "start"                         : "2022-05-25",
        "chain_id"                      : "1313161554",
        "native_token_symbol"           : "'ETH'",
        "wrapped_native_token_address"  : "0xc9bdeed33cd01541e1eed10f90519d2c06fe3feb",
        "explorer_link"                 : "'https://explorer.aurora.dev'",
        "fusion_settlement_addresses"   : ['0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0xd41b24bba51fac0e4827b6f94c0d6ddeb183cd64'],
        "escrow_factory_addresses"      : [],
        "atokens"                       : false,
    }) }}
{% endmacro %}

{% macro oneinch_klaytn_cfg_macro() %}
    {{ return({
        "name"                          : "klaytn",
        "start"                         : "2022-08-02",
        "chain_id"                      : "8217",
        "native_token_symbol"           : "'ETH'",
        "wrapped_native_token_address"  : "0x",
        "explorer_link"                 : "'https://klaytnscope.com'",
        "fusion_settlement_addresses"   : ['0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0xa218543cc21ee9388fa1e509f950fd127ca82155'],
        "escrow_factory_addresses"      : [],
        "atokens"                       : false,
    }) }}
{% endmacro %}

{% macro oneinch_zksync_cfg_macro() %}
    {{ return({
        "name"                          : "zksync",
        "start"                         : "2023-04-12",
        "chain_id"                      : "324",
        "native_token_symbol"           : "'ETH'",
        "wrapped_native_token_address"  : "0x5aea5775959fbc2557cc8789bc1bf90a239d9a91",
        "explorer_link"                 : "'https://explorer.zksync.io'",
        "fusion_settlement_addresses"   : ['0x8261425bf01caf25259dabe36fd05f430b38aee0', '0xfafc781997d41a42eb5023c103e562417524cfb6', '0x0302b42c86540e636e438395c6344ed88c55b70e', '0x11de482747d1b39e599f120d526af512dd1a9326'],
        "escrow_factory_addresses"      : ['0x584aeab186d81dbb52a8a14820c573480c3d4773'],
        "atokens"                       : true,
    }) }}
{% endmacro %}

{% macro oneinch_base_cfg_macro() %}
    {{ return({
        "name"                          : "base",
        "start"                         : "2023-08-08",
        "chain_id"                      : "8453",
        "native_token_symbol"           : "'ETH'",
        "wrapped_native_token_address"  : "0x4200000000000000000000000000000000000006",
        "explorer_link"                 : "'https://basescan.org'",
        "fusion_settlement_addresses"   : ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0x7f069df72b7a39bce9806e3afaf579e54d8cf2b9'],
        "escrow_factory_addresses"      : ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
        "atokens"                       : true,
    }) }}
{% endmacro %}

{% macro oneinch_linea_cfg_macro() %}
    {{ return({
        "name"                          : "linea",
        "start"                         : "2025-02-12",
        "chain_id"                      : "59144",
        "native_token_symbol"           : "'ETH'",
        "wrapped_native_token_address"  : "0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f",
        "explorer_link"                 : "'https://lineascan.build'",
        "fusion_settlement_addresses"   : ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840'],
        "escrow_factory_addresses"      : ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
        "atokens"                       : true,
    }) }}
{% endmacro %}

{% macro oneinch_sonic_cfg_macro() %}
    {{ return({
        "name"                          : "sonic",
        "start"                         : "2025-05-22",
        "chain_id"                      : "146",
        "native_token_symbol"           : "'SONIC'",
        "wrapped_native_token_address"  : "0x039e2fb66102314ce7b64ce5ce3e5183bc94ad38",
        "explorer_link"                 : "'https://sonicscan.org'",
        "fusion_settlement_addresses"   : ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e'],
        "escrow_factory_addresses"      : ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
        "atokens"                       : false,
    }) }}
{% endmacro %}

{% macro oneinch_unichain_cfg_macro() %}
    {{ return({
        "name"                          : "unichain",
        "start"                         : "2025-05-22",
        "chain_id"                      : "130",
        "native_token_symbol"           : "'ETH'",
        "wrapped_native_token_address"  : "0x4200000000000000000000000000000000000006",
        "explorer_link"                 : "'https://uniscan.xyz'",
        "fusion_settlement_addresses"   : ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e'],
        "escrow_factory_addresses"      : ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
        "atokens"                       : false,
    }) }}
{% endmacro %}

{% macro oneinch_solana_cfg_macro() %}
    {{ return({
        "name"                          : "solana",
        "start"                         : "2025-04-10",
        "chain_id"                      : "501",
        "native_token_symbol"           : "'SOL'",
        "wrapped_native_token_address"  : "from_base58('So11111111111111111111111111111111111111112')",
        "explorer_link"                 : "'https://solscan.io'",
    }) }}
{% endmacro %}



-- EXPOSED --

{% macro oneinch_blockchains_cfg_macro() %}
    {{ return([
        dict(oneinch_ethereum_cfg_macro()   , evm=true  , fusionV1=true , exposed=["ar", "lo", "cc"] , contracts=oneinch_meta_contracts_cfg_macro()),
        dict(oneinch_bnb_cfg_macro()        , evm=true  , fusionV1=true , exposed=["ar", "lo", "cc"] , contracts=oneinch_meta_contracts_cfg_macro()),
        dict(oneinch_polygon_cfg_macro()    , evm=true  , fusionV1=true , exposed=["ar", "lo", "cc"] , contracts=oneinch_meta_contracts_cfg_macro()),
        dict(oneinch_arbitrum_cfg_macro()   , evm=true  , fusionV1=true , exposed=["ar", "lo", "cc"] , contracts=oneinch_meta_contracts_cfg_macro()),
        dict(oneinch_optimism_cfg_macro()   , evm=true  , fusionV1=true , exposed=["ar", "lo", "cc"] , contracts=oneinch_meta_contracts_cfg_macro()),
        dict(oneinch_avalanche_c_cfg_macro(), evm=true  , fusionV1=true , exposed=["ar", "lo", "cc"] , contracts=oneinch_meta_contracts_cfg_macro()),
        dict(oneinch_gnosis_cfg_macro()     , evm=true  , fusionV1=true , exposed=["ar", "lo", "cc"] , contracts=oneinch_meta_contracts_cfg_macro()),
        dict(oneinch_base_cfg_macro()       , evm=true  , fusionV1=true , exposed=["ar", "lo", "cc"] , contracts=oneinch_meta_contracts_cfg_macro()),
        dict(oneinch_fantom_cfg_macro()     , evm=true  , fusionV1=true , exposed=["ar", "lo"]       , contracts={
            "AccessTokenFusionV1"       : oneinch_meta_contracts_cfg_macro().AccessTokenFusionV1,
            "AccessTokenCrossChainV1"   : oneinch_meta_contracts_cfg_macro().AccessTokenCrossChainV1,
        }),
        dict(oneinch_zksync_cfg_macro()     , evm=true  , fusionV1=false, exposed=["ar", "lo", "cc"], contracts={
            "AccessTokenLimitsV1"       : dict(oneinch_meta_contracts_cfg_macro().AccessTokenLimitsV1       , address="0x4888651051b2dc08ac55cd0f7d671e0fcba0deed"),
            "AccessTokenFusionV1"       : dict(oneinch_meta_contracts_cfg_macro().AccessTokenFusionV1       , address="0x46b64318c4f764f6fe81dfd1f26282a52e0f1680"),
            "AccessTokenCrossChainV1"   : dict(oneinch_meta_contracts_cfg_macro().AccessTokenCrossChainV1   , address="0xc2c4fe863ec835d7ddbfe91fe33cf1c7df45fa7c"),
        }),
        dict(oneinch_linea_cfg_macro()      , evm=true  , fusionV1=false, exposed=["ar", "lo", "cc"], contracts=oneinch_meta_contracts_cfg_macro()),
        dict(oneinch_sonic_cfg_macro()      , evm=true  , fusionV1=false, exposed=["ar", "lo", "cc"], contracts=oneinch_meta_contracts_cfg_macro()),
        dict(oneinch_unichain_cfg_macro()   , evm=true  , fusionV1=false, exposed=["ar", "lo", "cc"], contracts=oneinch_meta_contracts_cfg_macro()),
        dict(oneinch_aurora_cfg_macro()     , evm=true  , fusionV1=false),
        dict(oneinch_klaytn_cfg_macro()     , evm=true  , fusionV1=false),
        dict(oneinch_solana_cfg_macro()     , evm=false , fusionV1=false),
    ]) }}
{% endmacro %}