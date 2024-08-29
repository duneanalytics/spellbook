




{% macro get_abi(signature_id) %}
    {% set abi_query %}
        SELECT abi FROM abi.signatures WHERE id = '{{ signature_id }}'
    {% endset %}
    {% set results = run_query(abi_query) %}
    {% if execute %}
        {% set abi = results.columns[0].values()[0] %}
        {{ return(abi) }}
    {% else %}
        {{ return('') }}
    {% endif %}
{% endmacro %}

{% macro uniswap_v2_factory_mass_decoding(
    logs = null,
    signature_id = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9'
    )
%}

{% set abi = get_abi(signature_id) %}

SELECT
token0
,token1
,pair
,contract_address
,block_time
,block_number
,block_date
,tx_hash
,index

FROM TABLE (
    decode_evm_event (
      abi => '{{ abi }}',
      input => TABLE (
        SELECT l.* 
        FROM {{logs}} l
        WHERE topic0 = '{{signature_id}}'
        and block_date > (Select min(block_date) from {{logs}} where topic0 = '{{signature_id}}') -- take out limit if you want to use in prod
      )
    )
  )

{% endmacro %}

