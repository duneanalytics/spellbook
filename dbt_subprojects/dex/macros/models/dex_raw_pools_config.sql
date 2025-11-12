{# 
    Unified configuration for DEX pool parsing
    Separated into logs and traces configurations for better readability
#}



{% macro dex_raw_pools_logs_config_macro() %}
    {% set logs_configs = {
        '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9': {
            'type': 'uniswap_compatible',
            'version': 'v2',
            'pool': 'substr(data, 13, 20)',
            'token0': 'substr(topic1, 13, 20)',
            'token1': 'substr(topic2, 13, 20)',
            'fee': 'coalesce(bytearray_to_uint256(topic3), uint256 \'3000\')',
        },
        '0x3541d8fea55be35f686281f975bf8b7ab8fbb500c1c7ddd6c4e714655e9cd4e2': {
            'type': 'uniswap_compatible',
            'version': 'v2',
            'pool': 'substr(data, 13, 20)',
            'token0': 'substr(topic1, 13, 20)',
            'token1': 'substr(topic2, 13, 20)',
            'fee': 'coalesce(bytearray_to_uint256(topic3), uint256 \'3000\')',
        },
        '0x41f8736f924f57e464ededb08bf71f868f9d142885bbc73a1516db2be21fc428': {
            'type': 'uniswap_compatible',
            'version': 'v2',
            'pool': 'substr(data, 13, 20)',
            'token0': 'substr(topic1, 13, 20)',
            'token1': 'substr(topic2, 13, 20)',
            'fee': 'coalesce(bytearray_to_uint256(topic3), uint256 \'3000\')',
        },
        '0xc4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9': {
            'type': 'uniswap_compatible',
            'version': 'v2',
            'pool': 'substr(data, 45, 20)',
            'token0': 'substr(topic1, 13, 20)',
            'token1': 'substr(topic2, 13, 20)',
            'fee': 'coalesce(bytearray_to_uint256(topic3), uint256 \'3000\')',
        },
        '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118': {
            'type': 'uniswap_compatible',
            'version': 'v3',
            'pool': 'substr(data, 45, 20)',
            'token0': 'substr(topic1, 13, 20)',
            'token1': 'substr(topic2, 13, 20)',
            'fee': 'coalesce(bytearray_to_uint256(topic3), uint256 \'3000\')',
        },

        '0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2': {
            'type': 'aerodrome_compatible',
            'version': 'slipstream',
            'pool': 'substr(data, 13, 20)',
            'token0': 'substr(topic1, 13, 20)',
            'token1': 'substr(topic2, 13, 20)',
            'fee': 'null',
        },
        '0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e': {
            'type': 'aerodrome_compatible',
            'version': 'v1',
            'pool': 'substr(data, 13, 20)',
            'token0': 'substr(topic1, 13, 20)',
            'token1': 'substr(topic2, 13, 20)',
            'fee': 'null',
        },

        '0x9b3fb3a17b4e94eb4d1217257372dcc712218fcd4bc1c28482bd8a6804a7c775': {
            'type': 'maverick_compatible',
            'version': 'v1',
            'pool': 'substr(data, 13, 20)',
            'token0': 'substr(data, ' ~ (6*32 + 13) ~ ', 20)',
            'token1': 'substr(data, ' ~ (7*32 + 13) ~ ', 20)',
            'fee': 'null',
        },
        '0x848331e408557f4b7eb6561ca1c18a3ac43004fbe64b8b5bce613855cfdf22d2': {
            'type': 'maverick_compatible',
            'version': 'v2',
            'pool': 'substr(data, 13, 20)',
            'token0': 'substr(data, ' ~ (7*32 + 13) ~ ', 20)',
            'token1': 'substr(data, ' ~ (8*32 + 13) ~ ', 20)',
            'fee': 'null',
        }
    } %}
    {{ return(logs_configs) }}
{% endmacro %}



{% macro dex_raw_pools_traces_config_macro() %}
    {% set traces_configs = {
        '0x52f2db69': {
            'type': 'curve_compatible',
            'version': 'Factory V1 Plain',
            'pool': 'substr(output, 13, 20)',
            'tokens': 'transform(sequence(1, 32 * 4, 32), x -> substr(substr(substr(input, ' ~ (4 + 1 + 32 * 2) ~ ', 32 * 4), x, 32), 13))',
            'fee': 'bytearray_to_uint256(substr(input, ' ~ (4 + 1 + 32 * 7) ~ ', 32))',
        },
        '0xd4b9e214': {
            'type': 'curve_compatible',
            'version': 'Factory V1 Plain',
            'pool': 'substr(output, 13, 20)',
            'tokens': 'transform(sequence(1, 32 * 4, 32), x -> substr(substr(substr(input, ' ~ (4 + 1 + 32 * 2) ~ ', 32 * 4), x, 32), 13))',
            'fee': 'bytearray_to_uint256(substr(input, ' ~ (4 + 1 + 32 * 7) ~ ', 32))',
        },
        '0xcd419bb5': {
            'type': 'curve_compatible',
            'version': 'Factory V1 Plain',
            'pool': 'substr(output, 13, 20)',
            'tokens': 'transform(sequence(1, 32 * 4, 32), x -> substr(substr(substr(input, ' ~ (4 + 1 + 32 * 2) ~ ', 32 * 4), x, 32), 13))',
            'fee': 'bytearray_to_uint256(substr(input, ' ~ (4 + 1 + 32 * 7) ~ ', 32))',
        },
        '0x5c16487b': {
            'type': 'curve_compatible',
            'version': 'Factory V1 Plain',
            'pool': 'substr(output, 13, 20)',
            'tokens': 'transform(sequence(1, 32 * 4, 32), x -> substr(substr(substr(input, ' ~ (4 + 1 + 32 * 2) ~ ', 32 * 4), x, 32), 13))',
            'fee': 'bytearray_to_uint256(substr(input, ' ~ (4 + 1 + 32 * 7) ~ ', 32))',
        },
        '0xc955fa04': {
            'type': 'curve_compatible',
            'version': 'Factory V2',
            'pool': 'substr(output, 13, 20)',
            'tokens': 'transform(sequence(1, 32 * 2, 32), x -> substr(substr(substr(input, ' ~ (4 + 1 + 32 * 2) ~ ', 32 * 2), x, 32), 13))',
            'fee': 'bytearray_to_uint256(substr(input, ' ~ (4 + 1 + 32 * 4) ~ ', 32))',
        },
        '0xaa38b385': {
            'type': 'curve_compatible',
            'version': 'Factory V2',
            'pool': 'substr(output, 13, 20)',
            'tokens': 'transform(sequence(1, 32 * 3, 32), x -> substr(substr(substr(input, ' ~ (4 + 1 + 32 * 2) ~ ', 32 * 3), x, 32), 13))',
            'fee': 'cast(null as uint256)',
        },
        '0x5bcd3d83': {
            'type': 'curve_compatible',
            'version': 'Factory V1 Plain Stableswap',
            'pool': 'substr(output, 13, 20)',
            'tokens': 'transform(sequence(1, 32 * 8, 32), x -> substr(substr(substr(input, ' ~ (4 + 1 + 32 * 16) ~ ', 32 * 8), x, 32), 13))',
            'fee': 'bytearray_to_uint256(substr(input, ' ~ (4 + 1 + 32 * 4) ~ ', 32))',
        },
        '0x485cc955': {
            'type': 'uniswap_compatible',
            'version': 'v2',
            'pool': '"to"',
            'token0': 'substr(input, 17, 20)',
            'token1': 'substr(input, 49, 20)',
            'initialization_call': True,
        } 
    } %}
    {{ return(traces_configs) }}
{% endmacro %}



{% macro dex_raw_pools_blockchains_macro() %}
    {% set blockchains = [
        "ethereum",
        "bnb",
        "polygon",
        "avalanche_c",
        "gnosis",
        "fantom",
        "optimism",
        "arbitrum",
        "celo",
        "base",
        "zksync",
        "zora",
        "sonic",
        "linea",
        "unichain",
    ] %}
    {{ return(blockchains) }}
{% endmacro %}



-- will be included later
-- '0xde7fe3bf': {
--     'type': 'curve_compatible',
--     'version': 'Factory V2 Meta',
--     'pool': 'substr(output, 13, 20)',
--     'base_pool': 'substr(input, ' ~ (4 + 1 + 13) ~ ', 20)',
--     'coin': 'substr(input, ' ~ (4 + 1 + 32 * 3 + 13) ~ ', 20)',
--     'skip': True,
-- },
-- '0xe339eb4f': {
--     'type': 'curve_compatible',
--     'version': 'Factory V2 Meta',
--     'pool': 'substr(output, 13, 20)',
--     'base_pool': 'substr(input, ' ~ (4 + 1 + 13) ~ ', 20)',
--     'coin': 'substr(input, ' ~ (4 + 1 + 32 * 3 + 13) ~ ', 20)',
--     'skip': True,
-- }
