{% macro evm_event_decoding_base(
    logs = null,
    abi = null,
    topic0 = null
    )
%}

SELECT
*

FROM TABLE (
    decode_evm_event (
      abi => '{{abi}}',
      input => TABLE (
        SELECT l.* 
        FROM {{logs}} l
        WHERE topic0 = {{topic0}}
        and block_date > (Select min(block_date) from {{logs}} where topic0 = {{topic0}})
      )
    )
  )

{% endmacro %}