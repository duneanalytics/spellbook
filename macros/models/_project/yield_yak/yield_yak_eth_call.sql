{%- macro yield_yak_eth_call(
        blockchain = null,
        address_column_name = null,
        function_signature = null
    )
-%}

{%- if blockchain == 'avalanche_c' -%}
{%- set target_url = 'https://rpc.ankr.com/avalanche' -%}
{%- elif blockchain == 'arbitrum' -%}
{%- set target_url = 'https://rpc.ankr.com/arbitrum' -%}
{%- else -%}
{%- set target_url = 'https://rpc.ankr.com/' + blockchain -%}
{%- endif -%}

{#-
Putting this onto a single line because it looks messy otherwise after running dbt compile
It runs eth_call on the address_column_name for the given function signature and appropriate target_url.
This returns a JSON object so we then retrieve the result using json_extra_scalar and then convert it
to varbinary using from_hex
-#}

from_hex(json_extract_scalar(http_post('{{ target_url }}', concat('{"id":1,"jsonrpc":"2.0","method":"eth_call","params":[{"to":"', CAST({{ address_column_name }} AS varchar), '","data":"{{ function_signature }}"},"latest"]}'), ARRAY['Content-Type: application/json']), '$.result'))

{%- endmacro -%}