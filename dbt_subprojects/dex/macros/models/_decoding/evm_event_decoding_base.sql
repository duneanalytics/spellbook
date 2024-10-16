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
            {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
            {% else %}
            AND block_date >= (SELECT MIN(block_date) FROM {{logs}} WHERE topic0 = {{topic0}})
            {% endif %}
      )
    )
  )

{% endmacro %}
