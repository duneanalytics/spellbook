{%- macro yield_yak_sub_wrapped_native_token(
        blockchain = null,
        address_column_name = null
    )
-%}
{#- This is used to substitute wrapped versions of tokens when we are getting token information so that we can then track the prices -#}

{%- if blockchain == 'avalanche_c' -%}
{#- WAVAX -#}
{%- set wrapped_native_token = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' -%}
{%- elif blockchain == 'arbitrum' -%}
{#- WETH -#}
{%- set wrapped_native_token = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' -%}
{%- else -%}
{%- set wrapped_native_token = '0x0000000000000000000000000000000000000000' -%}
{%- endif -%}

CASE WHEN {{ address_column_name }} = 0x0000000000000000000000000000000000000000 THEN {{ wrapped_native_token }} ELSE {{ address_column_name }} END

{%- endmacro -%}