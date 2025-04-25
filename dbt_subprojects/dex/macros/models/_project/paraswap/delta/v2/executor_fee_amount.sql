{% macro executor_fee_amount() %}
CASE
    WHEN executor = 0x6bb000067005450704003100632eb93ea00c0000 THEN varbinary_to_uint256(varbinary_substring(executorData,  161, 32))
    -- WHEN executor = 0x0500b5050c40e06ed700005dd7cb0ef0b0d0a000 THEN "TODO: join augustus executor contract call and return the output"
    ELSE 0
END as "executorFeeAmount"
{% endmacro %}