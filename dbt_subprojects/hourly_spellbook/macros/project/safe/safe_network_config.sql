{% macro safe_network_config() %}
    {%- set networks = {
        'arbitrum': {
            'start_date': '2021-06-20',
            'native_token': 'ETH',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
                'SafeProxyFactory_v1_4_1_evt_ProxyCreation'
            ]
        },
        'avalanche_c': {
            'start_date': '2021-07-29',
            'native_token': 'AVAX',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
            ]
        },
        'base': {
            'start_date': '2023-06-15',
            'native_token': 'ETH',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
                'SafeProxyFactory_v1_4_1_evt_ProxyCreation'
            ]
        },
        'berachain': {
            'start_date': '2024-01-11',
            'native_token': 'BERA',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
                'SafeProxyFactory_v1_4_1_evt_ProxyCreation'
            ],
            'has_native_transfers': false
        },
        'blast': {
            'start_date': '2024-02-24',
            'native_token': 'ETH',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
                'SafeProxyFactory_v1_4_1_evt_ProxyCreation'
            ]
        },
        'bnb': {
            'start_date': '2021-01-29',
            'native_token': 'BNB',
            'legacy_singleton_sources': [
                {'table': 'ProxyFactory_v1_1_1_call_createProxy', 'column': 'masterCopy'},
                {'table': 'ProxyFactory_v1_1_1_call_createProxyWithNonce', 'column': '_mastercopy'},
                {'table': 'ProxyFactory_v1_1_1_call_createProxyWithCallback', 'column': '_mastercopy'}
            ],
            'singleton_type': 'legacy',
            'modern_singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
            ]
        },
        'celo': {
            'start_date': '2021-07-20',
            'native_token': 'CELO',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
            ]
        },
        'ethereum': {
            'start_date': '2018-11-24',
            'native_token': 'ETH',
            'singleton_type': 'legacy_ethereum'
        },
        'fantom': {
            'start_date': '2021-12-20',
            'native_token': 'FTM',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
            ],
            'has_native_transfers': false
        },
        'gnosis': {
            'start_date': '2020-05-15',
            'native_token': 'xDAI',
            'legacy_singleton_sources': [
                {'table': 'ProxyFactory_v1_1_1_call_createProxy', 'column': 'masterCopy'},
                {'table': 'ProxyFactory_v1_1_1_call_createProxyWithNonce', 'column': '_mastercopy'},
                {'table': 'ProxyFactory_v1_1_1_call_createProxyWithCallback', 'column': '_mastercopy'}
            ],
            'singleton_type': 'legacy',
            'modern_singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation'
            ]
        },
        'linea': {
            'start_date': '2023-07-11',
            'native_token': 'ETH',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
                'SafeProxyFactory_v1_4_1_evt_ProxyCreation'
            ]
        },
        'mantle': {
            'start_date': '2023-07-14',
            'native_token': 'MNT',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
                'SafeProxyFactory_v1_4_1_evt_ProxyCreation'
            ]
        },
        'optimism': {
            'start_date': '2021-11-17',
            'native_token': 'ETH',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
                'SafeProxyFactory_v1_4_1_evt_ProxyCreation'
            ]
        },
        'polygon': {
            'start_date': '2021-03-07',
            'native_token': 'MATIC',
            'singleton_type': 'legacy',
            'legacy_singleton_sources': [
                {'table': 'ProxyFactory_v1_1_1_call_createProxy', 'column': 'masterCopy'},
                {'table': 'ProxyFactory_v1_1_1_call_createProxyWithNonce', 'column': '_mastercopy'},
                {'table': 'ProxyFactory_v1_1_1_call_createProxyWithCallback', 'column': '_mastercopy'}
            ],
            'modern_singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
                'SafeProxyFactory_v1_4_1_evt_ProxyCreation'
            ]
        },
        'ronin': {
            'start_date': '2024-03-19',
            'native_token': 'ETH',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
            ]
        },
        'scroll': {
            'start_date': '2023-10-10',
            'native_token': 'ETH',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
                'SafeProxyFactory_v1_4_1_evt_ProxyCreation'
            ]
        },
        'unichain': {
            'start_date': '2024-11-19',
            'native_token': 'ETH',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
                'SafeProxyFactory_v1_4_1_evt_ProxyCreation'
            ]
        },
        'worldchain': {
            'start_date': '2024-10-21',
            'native_token': 'ETH',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
                'SafeProxyFactory_v1_4_1_evt_ProxyCreation'
            ]
        },
        'zkevm': {
            'start_date': '2023-03-24',
            'native_token': 'MATIC',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
                'SafeProxyFactory_v1_4_1_evt_ProxyCreation'
            ]
        },
        'zksync': {
            'start_date': '2023-02-14',
            'native_token': 'ETH',
            'singleton_type': 'modern',
            'singleton_sources': [
                'SafeProxyFactory_v1_3_0_evt_ProxyCreation',
            ]
        }
    } -%}
    {{ return(networks) }}
{% endmacro %}

{% macro get_safe_network_config(blockchain) %}
    {%- set all_networks = safe_network_config() -%}
    {%- if blockchain in all_networks -%}
        {{ return(all_networks[blockchain]) }}
    {%- else -%}
        {{ exceptions.raise_compiler_error("Network '" ~ blockchain ~ "' not found in safe_network_config") }}
    {%- endif -%}
{% endmacro %}

{% macro get_safe_contributors(blockchain=none, model_type=none) %}
    {#-
    Centralized contributor list for Safe project
    Returns the list of all Safe contributors
    #}
    {%- set default_contributors = ["tschubotz", "peterrliem", "danielpartida", "hosuke", "frankmaseo", "kryptaki", "sche", "safehjc"] -%}
    {{ return(default_contributors) }}
{% endmacro %}
