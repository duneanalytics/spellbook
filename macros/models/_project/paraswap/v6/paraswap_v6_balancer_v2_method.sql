{% macro paraswap_v6_balancer_v2_method(tableOuter, tableInner, srcTable, inOrOut, method ) %}
WITH
                  {{ tableOuter }} as (
                    WITH
                      {{tableInner}} AS (
                        SELECT
                          *,
                          try_cast(
                            varbinary_to_uint256 (from_hex(s2.sData[s2.assetsOffset])) as integer
                          ) as assetsSize,
                          try_cast(
                            varbinary_to_uint256 (from_hex(s2.sData[s2.limitOffset])) as integer
                          ) as limitSize
                        FROM
                          (
                            SELECT
                              *,
                              try_cast(
                                varbinary_to_uint256 (from_hex(s1.sData[3])) as integer
                              ) / 32 + 1 as assetsOffset,
                              try_cast(
                                varbinary_to_uint256 (from_hex(s1.sData[8])) as integer
                              ) / 32 + 1 as limitOffset
                            FROM
                              (
                                SELECT
                                  *,
                                  substr(try_cast(data as varchar), 11), --dbl ch -eck if need
                                  regexp_extract_all(substr(try_cast(data as varchar), 11), '.{64}') as sData
                                FROM
                                  {{ srcTable }} 
                                  {% if is_incremental() %}
                                    WHERE call_block_time >= date_trunc('day', now() - interval '7' day)
                                  {% endif %}
                              ) AS s1
                          ) as s2
                      )
                    SELECT
                      *,{% if inOrOut == 'in' %}
                      sData[assetsOffset + 1] as srcToken,
                      sData[assetsOffset + assetsSize] as destToken,
                      varbinary_to_uint256 (from_hex(sData[limitOffset + 1])) as fromAmount,
                      try_cast(
                        - varbinary_to_int256 (from_hex(sData[limitOffset + limitSize])) as uint256
                      ) as toAmount,{% elif inOrOut == 'out' %}
                      sData[assetsOffset + assetsSize] as srcToken,
                      sData[assetsOffset + 1] as destToken,
                      try_cast(
                        varbinary_to_int256 (from_hex(sData[limitOffset + limitSize])) as uint256
                      ) as fromAmount,
                      try_cast(
                        (
                          - varbinary_to_int256 (from_hex(sData[limitOffset + 1]))
                        ) as uint256
                      ) as toAmount,{% endif %}                                              
                      to_hex(
                        try_cast(
                          bitwise_and(
                            try_cast(
                              JSON_EXTRACT_SCALAR(balancerData, '$.beneficiaryAndApproveFlag') AS UINT256
                            ),
                            varbinary_to_uint256 (0xffffffffffffffffffffffffffffffffffffffff)
                          ) as VARBINARY
                        )
                      ) as beneficiary
                    FROM
                      {{ tableInner }}
                  )
select
              call_block_time,
                          call_block_number,
                          call_tx_hash,
                          contract_address as project_contract_address,
                          call_trace_address,
                          case
                            when try_cast(srcToken as uint256) = uint256 '0' then '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                            else try_cast(
                              from_hex(regexp_replace(srcToken, '(00){12}')) as varchar
                            ) -- shrink address to to bytes20
                          end as srcToken,
                          case
                            when try_cast(destToken as uint256) = uint256 '0' then '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                            else try_cast(
                              from_hex(regexp_replace(destToken, '(00){12}')) as varchar
                            ) -- shrink address to to bytes20
                          end as destToken,
                          fromAmount,
                          toAmount,
                          try_cast(
                            JSON_EXTRACT_SCALAR(balancerData, '$.quotedAmount') as uint256
                          ) AS quotedAmount,
                          output_receivedAmount,
                          JSON_EXTRACT_SCALAR(balancerData, '$.metadata') AS metadata,
                          try_cast(
                            from_hex(regexp_replace(beneficiary, '(00){12}')) as varchar
                          ) as beneficiary,
                          partnerAndFee,
                          output_partnerShare,
                          output_paraswapShare,
                          '{{ method }}' as method{% if inOrOut == 'out' %},
                          output_spentAmount as spentAmount{% endif %}
            from
              {{tableOuter}}              
{% endmacro %}
