{% macro oneinch_meta_cfg_macro(property = none) %}

-- STREAMS CONFIG --
-- start dates for sreams by default:
-- for ar: 2019-06-01
-- for lo: 2021-06-01 / fusion: 2022-12-25
-- for cc: 2024-08-20
-- for a quick CI, change the start dates of the streams to light/easy
{%
    set streams = {
        "ar": {
            "start": {
                "raw_calls" : "2025-09-10",
                "transfers" : "2025-09-10",
                "stream"    : "2025-09-10",
                "executions": "2025-09-10",
            },
            "contracts" : oneinch_ar_cfg_contracts_macro(),
        },
        "lo": {
            "start": {
                "raw_calls" : "2025-09-10",
                "transfers" : "2025-09-10",
                "stream"    : "2025-09-10",
                "executions": "2025-09-10",
                "fusion"    : "2025-09-10",
            },
            "contracts" : oneinch_lo_cfg_contracts_macro(),
        },
        "cc": {
            "start": {
                "raw_calls" : "2025-09-10",
                "transfers" : "2025-09-10",
                "stream"    : "2025-09-10",
                "executions": "2025-09-10",
            },
            "contracts" : oneinch_cc_cfg_contracts_macro(),
        },
    }
%}

-- BLOCKCHAINS CONFIG --
{%
    set blockchains = {
        "exposed": {
            "ethereum":     "evms",
            "bnb":          "evms",
            "polygon":      "evms",
            "arbitrum":     "evms",
            "optimism":     "evms",
            "avalanche_c":  "evms",
            "gnosis":       "evms",
            "fantom":       "evms",
            "base":         "evms",
            "zksync":       "evms",
            "linea":        "evms",
            "sonic":        "evms",
            "unichain":     "evms",
            "solana":       "solana",
        },
        "start": {
            "ethereum":     "2019-06-03",
            "bnb":          "2021-02-18",
            "polygon":      "2021-05-05",
            "arbitrum":     "2021-06-22",
            "optimism":     "2021-11-12",
            "avalanche_c":  "2021-12-22",
            "gnosis":       "2021-12-22",
            "fantom":       "2022-03-16",
            "aurora":       "2022-05-25",
            "klaytn":       "2022-08-02",
            "zksync":       "2023-04-12",
            "base":         "2023-08-08",
            "linea":        "2025-02-12",
            "solana":       "2025-04-10",
            "sonic":        "2025-05-22",
            "unichain":     "2025-05-22",
        },
        "chain_id": {
            "ethereum":     "1",
            "bnb":          "56",
            "polygon":      "137",
            "arbitrum":     "42161",
            "optimism":     "10",
            "avalanche_c":  "43114",
            "gnosis":       "100",
            "fantom":       "250",
            "base":         "8453",
            "zksync":       "324",
            "aurora":       "1313161554",
            "klaytn":       "8217",
            "linea":        "59144",
            "sonic":        "146",
            "unichain":     "130",
        },
        "native_token_symbol": {
            "ethereum":     "ETH",
            "bnb":          "BNB",
            "polygon":      "MATIC",
            "arbitrum":     "ETH",
            "optimism":     "ETH",
            "avalanche_c":  "AVAX",
            "gnosis":       "xDAI",
            "fantom":       "FTM",
            "base":         "ETH",
            "zksync":       "ETH",
            "aurora":       "ETH",
            "klaytn":       "ETH",
            "linea":        "ETH",
            "sonic":        "SONIC",
            "unichain":     "ETH",
        },
        "wrapped_native_token_address": {
            "ethereum":     "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "bnb":          "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
            "polygon":      "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
            "arbitrum":     "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "optimism":     "0x4200000000000000000000000000000000000006",
            "avalanche_c":  "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
            "gnosis":       "0xe91d153e0b41518a2ce8dd3d7944fa863463a97d",
            "fantom":       "0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83",
            "base":         "0x4200000000000000000000000000000000000006",
            "zksync":       "0x5aea5775959fbc2557cc8789bc1bf90a239d9a91",
            "aurora":       "0xc9bdeed33cd01541e1eed10f90519d2c06fe3feb",
            "linea":        "0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f",
            "sonic":        "0x039e2fb66102314ce7b64ce5ce3e5183bc94ad38",
            "unichain":     "0x4200000000000000000000000000000000000006",
            "klaytn":       "0x",
        },
        "explorer_link": {
            "ethereum":     "https://etherscan.io",
            "bnb":          "https://bscscan.com",
            "polygon":      "https://polygonscan.com",
            "arbitrum":     "https://arbiscan.io",
            "optimism":     "https://explorer.optimism.io",
            "avalanche_c":  "https://snowtrace.io",
            "gnosis":       "https://gnosisscan.io",
            "fantom":       "https://ftmscan.com",
            "base":         "https://basescan.org",
            "zksync":       "https://explorer.zksync.io",
            "aurora":       "https://explorer.aurora.dev",
            "klaytn":       "https://klaytnscope.com",
            "linea":        "https://lineascan.build",
            "sonic":        "https://sonicscan.org",
            "unichain":     "https://uniscan.xyz",
        },
        "fusion_settlement_addresses": {
            "ethereum":     ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0xa88800cd213da5ae406ce248380802bd53b47647'],
            "bnb":          ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0x1d0ae300eec4093cee4367c00b228d10a5c7ac63'],
            "polygon":      ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0x1e8ae092651e7b14e4d0f93611267c5be19b8b9f'],
            "arbitrum":     ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0x4bc3e539aaa5b18a82f6cd88dc9ab0e113c63377'],
            "optimism":     ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0xd89adc20c400b6c45086a7f6ab2dca19745b89c2'],
            "avalanche_c":  ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0x7731f8df999a9441ae10519617c24568dc82f697'],
            "gnosis":       ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0xcbdb7490968d4dbf183c60fc899c2e9fbd445308'],
            "base":         ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0x7f069df72b7a39bce9806e3afaf579e54d8cf2b9'],
            "zksync":       ['0x8261425bf01caf25259dabe36fd05f430b38aee0', '0xfafc781997d41a42eb5023c103e562417524cfb6', '0x0302b42c86540e636e438395c6344ed88c55b70e', '0x11de482747d1b39e599f120d526af512dd1a9326'],
            "linea":        ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840'],
            "fantom":       ['0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0xa218543cc21ee9388fa1e509f950fd127ca82155'],
            "aurora":       ['0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0xd41b24bba51fac0e4827b6f94c0d6ddeb183cd64'],
            "klaytn":       ['0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', '0xfb2809a5314473e1165f6b58018e20ed8f07b840', '0xa218543cc21ee9388fa1e509f950fd127ca82155'],
            "sonic":        ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e'],
            "unichain":     ['0x2ad5004c60e16e54d5007c80ce329adde5b51ef5', '0xabd4e5fb590aa132749bbf2a04ea57efbaac399e'],
        },
        "escrow_factory_addresses": {
            "ethereum":     ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
            "bnb":          ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
            "polygon":      ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
            "arbitrum":     ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a', '0xc02e6487fbf69d6849b4b9ad9ec0bf5ff8d0c2a1'],
            "optimism":     ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
            "avalanche_c":  ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
            "gnosis":       ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
            "base":         ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
            "linea":        ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
            "sonic":        ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
            "unichain":     ['0xa7bcb4eac8964306f9e3764f67db6a7af6ddf99a'],
            "zksync":       ['0x584aeab186d81dbb52a8a14820c573480c3d4773'],
            "fantom":       [],
            "aurora":       [],
            "klaytn":       [],
        },
    }
%}

{% set config = {
    "streams": streams,
    "blockchains": blockchains
} %}

{% if property is not none %}
    {{ return(config[property]) }}
{% endif %}

{{ return(config) }}

{% endmacro %}