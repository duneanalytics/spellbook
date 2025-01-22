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
        SELECT  block_number,
                block_time,
                block_date,
                date_trunc('month', block_date) as block_month,
                block_hash,
                contract_address, 
                topic0,
                topic1,
                topic2,
                topic3,
                data,
                tx_hash, 
                index as evt_index,
                tx_index, 
                tx_from,
                tx_to
        FROM {{logs}} l
        WHERE topic0 = {{topic0}}
            {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
            {% endif %}
      )
    )
  )

{% endmacro %}
