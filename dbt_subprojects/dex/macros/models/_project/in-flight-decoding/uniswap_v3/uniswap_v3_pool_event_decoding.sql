{% macro uniswap_v3_pool_event_decoding(logs) %}

{% set abi = '
{"name":"Swap","type":"event","inputs":[{"name":"sender","type":"address","indexed":true,"internalType":"address"},{"name":"recipient","type":"address","indexed":true,"internalType":"address"},{"name":"amount0","type":"int256","indexed":false,"internalType":"int256"},{"name":"amount1","type":"int256","indexed":false,"internalType":"int256"},{"name":"sqrtPriceX96","type":"uint160","indexed":false,"internalType":"uint160"},{"name":"liquidity","type":"uint128","indexed":false,"internalType":"uint128"},{"name":"tick","type":"int24","indexed":false,"internalType":"int24"}],"anonymous":false}
' %}

{% set topic0 = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67' %}

{{ evm_event_decoding_base(logs, abi, topic0) }}

{% endmacro %}

