{% macro uniswap_v3_factory_mass_decoding(logs) %}

{% set abi = '
{"name":"PoolCreated","type":"event","inputs":[{"name":"token0","type":"address","indexed":true,"internalType":"address"},{"name":"token1","type":"address","indexed":true,"internalType":"address"},{"name":"fee","type":"uint24","indexed":true,"internalType":"uint24"},{"name":"tickSpacing","type":"int24","indexed":false,"internalType":"int24"},{"name":"pool","type":"address","indexed":false,"internalType":"address"}],"anonymous":false}
' %}

{% set topic0 = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118' %}

{{ evm_event_decoding_base(logs, abi, topic0) }}

{% endmacro %}

