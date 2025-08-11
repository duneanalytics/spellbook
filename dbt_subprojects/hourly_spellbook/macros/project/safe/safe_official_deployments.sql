{% macro safe_chain_id_mapping() %}
    {#- 
    Maps our network names to official chain IDs used in safe-deployments repo
    Source: https://github.com/safe-global/safe-deployments
    Chain ID reference: https://chainlist.org/
    
    Note: Some networks are testnets:
    - berachain: 80084 (Bartio testnet)
    - unichain: 1301 (Sepolia testnet)
    - goerli: 5 (Deprecated testnet)
    #}
    {%- set chain_mapping = {
        'ethereum': 1,
        'optimism': 10,
        'bnb': 56,
        'gnosis': 100,
        'polygon': 137,
        'fantom': 250,
        'base': 8453,
        'arbitrum': 42161,
        'celo': 42220,
        'avalanche_c': 43114,
        'linea': 59144,
        'blast': 81457,
        'mantle': 5000,
        'worldchain': 480,
        'zksync': 324,
        'zkevm': 1101,
        'scroll': 534352,
        'berachain': 80084,
        'ronin': 2020,
        'unichain': 1301,
        'aurora': 1313161554,
        'moonbeam': 1284,
        'moonriver': 1285,
        'kava': 2222,
        'metis': 1088,
        'boba': 288,
        'okc': 66,
        'harmony': 1666600000,
        'fuse': 122,
        'cronos': 25,
        'evmos': 9001,
        'klaytn': 8217,
        'milkomeda': 2001,
        'thundercore': 108,
        'telos': 40,
        'rsk': 30,
        'songbird': 19,
        'shardeum': 8082,
        'neon': 245022934,
        'velas': 106,
        'oasis': 42262,
        'canto': 7700,
        'conflux': 1030,
        'xdc': 50,
        'meter': 82,
        'godwoken': 71402,
        'elastos': 20,
        'sepolia': 11155111,
        'holesky': 17000,
        'goerli': 5
    } -%}
    {{ return(chain_mapping) }}
{% endmacro %}

{% macro safe_official_singleton_addresses() %}
    {#- 
    Official Safe singleton addresses from safe-deployments repo
    These are the canonical addresses that are the same across most chains
    Source: https://github.com/safe-global/safe-deployments
    Last updated: 2024 (includes v1.5.0)
    
    Version History:
    ================
    v1.0.0 (2019) - Initial GnosisSafe release
    v1.1.1 (2020) - Bug fixes and improvements
    v1.2.0 (2020) - EIP-1271 signature validation support
    v1.3.0 (2021) - Major upgrade with contract guards, L2 support
    v1.4.0 (2023) - Rebranding to Safe, improved gas efficiency
    v1.4.1 (2023) - Minor improvements
    v1.5.0 (2024) - Latest version with passkey support
    #}
    {%- set singletons = {
        'v1.0.0': {
            'GnosisSafe': '0xb6029EA3B2c51D09a50B53CA8012FeEB05bDa35A',
            'ProxyFactory': '0x12302fE9c02ff50939BaAaaf415fc226C078613C'
        },
        'v1.1.1': {
            'GnosisSafe': '0x34CfAC646f301356fAa8B21e94227e3583Fe3F5F',
            'ProxyFactory': '0x76E2cFc1F5Fa8F6a5b3fC4c8F4788F0116861F9B'
        },
        'v1.2.0': {
            'GnosisSafe': '0x6851D6fDFAfD08c0295C392436245E5bc78B0185',
            'ProxyFactory': '0x88627c8904eCd9DF96A572Ef32E8a5Dd5D8c22E5'
        },
        'v1.3.0': {
            'GnosisSafe': '0xd9Db270c1B5E3Bd161E8c8503c55cEABeE709552',
            'GnosisSafeL2': '0x3E5c63644E683549055b9Be8653de26E0B4CD36E',
            'ProxyFactory': '0xa6B71E26C5e0845f74c812102Ca7114b6a896AB2',
            'ProxyFactory_eip155': '0xC22834581EbC8527d974F8a1c97E1bEA4EF910BC'
        },
        'v1.4.0': {
            'Safe': '0x41675C099F32341bf84BFc5382aF534df5C7461a',
            'SafeL2': '0x29fcB43b46531BcA003ddC8FCB67FFE91900C762'
        },
        'v1.4.1': {
            'Safe': '0x41675C099F32341bf84BFc5382aF534df5C7461a',
            'SafeL2': '0x29fcB43b46531BcA003ddC8FCB67FFE91900C762',
            'SafeProxyFactory': '0x4e1DCf7AD4e460CfD30791CCC4F9c8a4f820ec67'
        },
        'v1.5.0': {
            'Safe': '0xFf51A5898e281Db6DfC7855790607438dF2ca44b',
            'SafeL2': '0xEdd160fEBBD92E350D4D398fb636302fccd67C7e',
            'SafeProxyFactory': '0x23cDa853e1E9539C72890f0da313348011409260'
        }
    } -%}
    {{ return(singletons) }}
{% endmacro %}

{% macro validate_singleton_address(address, network) %}
    {#- 
    Validates if a singleton address matches official Safe deployments
    Returns true if the address is an official Safe singleton
    #}
    {%- set official = safe_official_singleton_addresses() -%}
    {%- set lower_address = address|lower -%}
    
    {%- for version, contracts in official.items() -%}
        {%- for contract_name, official_address in contracts.items() -%}
            {%- if lower_address == official_address|lower -%}
                {{ return(true) }}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
    
    {{ return(false) }}
{% endmacro %}

{% macro get_safe_version_from_singleton(address) %}
    {#- 
    Returns the Safe version based on singleton address
    #}
    {%- set official = safe_official_singleton_addresses() -%}
    {%- set lower_address = address|lower -%}
    
    {%- for version, contracts in official.items() -%}
        {%- for contract_name, official_address in contracts.items() -%}
            {%- if lower_address == official_address|lower -%}
                {{ return(version) }}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
    
    {{ return('unknown') }}
{% endmacro %}