{% macro paraswap_v6_balancer_v2_method(tableOuter, tableInner, srcTable, inOrOut, method ) %}
WITH
                  {{ tableOuter }} as (
                    WITH
                      {{tableInner}} AS (
                        SELECT
                          *,
                          substr(try_cast(data as varchar), 1, 10) as balancerV2Selector,
                          /* cutting down balancer data into bytes32 words to RLP decode assetOffset and assetsSize later */
                          regexp_extract_all(substr(try_cast(data as varchar), 11), '.{64}') as sData
                        FROM
                          {{ srcTable }}
                          WHERE
                            call_success = TRUE
                            {% if is_incremental() %}
                              AND {{ incremental_predicate('call_block_time') }}
                            {% endif %}
                      )
                    SELECT
                      *,{% if inOrOut == 'in' %}
                      CASE
                        ----------------------- BalancerV2Vault.swap()
                        WHEN balancerV2Selector = '0x52bbbe29' THEN substr(try_cast(data as varchar), 587, 64)
                        ----------------------- BalancerV2Vault.batchSwap()
                        WHEN balancerV2Selector = '0x945bcec9' THEN (
                          --- pick src_token from data[assetOffset + 1] (+1 since indices start from 1)
                          sData[
                            (
                              /* start of assetsOffset */
                              try_cast(
                                varbinary_to_uint256 (from_hex(sData[3])) as integer
                                ) / 32 + 1
                              /* end of assetsOffset */
                            ) + 1
                          ]
                        )
                        ELSE '000000000000000000000000000000000000000000000000000000000000dead' -- placeholder, contract logic prevents hitting this case. Same applies later
                      END AS src_token,
                      CASE
                        WHEN balancerV2Selector = '0x52bbbe29' THEN substr(try_cast(data as varchar), 651, 64)
                        WHEN balancerV2Selector = '0x945bcec9' THEN (
                          --- pick dest_token from data[assetOffset + assetsSize]
                          sData[
                            (
                              /* start of assetsOffset */
                              try_cast(
                                varbinary_to_uint256 (from_hex(sData[3])) as integer
                              ) / 32 + 1
                              /* end of assetsOfsset */
                            )
                            +
                            /* start of assetsSize */
                            try_cast(
                              varbinary_to_uint256 (
                                from_hex(
                                  sData[
                                    /* start of assetsOffset */
                                    try_cast(
                                      varbinary_to_uint256 (from_hex(sData[3])) as integer
                                    ) / 32 + 1
                                    /* end of assetsOfsset */
                                  ]
                                )
                              ) as integer
                            )
                            /* end of assetsSize */
                          ]
                        )
                        ELSE '000000000000000000000000000000000000000000000000000000000000dead'
                      END as dest_token,{% elif inOrOut == 'out' %}
                      CASE
                        WHEN balancerV2Selector = '0x52bbbe29' THEN substr(try_cast(data as varchar), 587, 64)
                        WHEN balancerV2Selector = '0x945bcec9' THEN (
                          --- pick dest_token from data[assetOffset + assetsSize]
                          sData[
                            (
                              /* start of assetsOffset */
                              try_cast(
                                varbinary_to_uint256 (from_hex(sData[3])) as integer
                              ) / 32 + 1
                              /* end of assetsOfsset */
                            )
                            +
                            /* start of assetsSize */
                            try_cast(
                              varbinary_to_uint256 (
                                from_hex(
                                  sData[
                                    /* start of assetsOffset */
                                    try_cast(
                                      varbinary_to_uint256 (from_hex(sData[3])) as integer
                                    ) / 32 + 1
                                    /* end of assetsOfsset */
                                  ]
                                )
                              ) as integer
                            )
                            /* end of assetsSize */
                          ]
                        )
                        ELSE '000000000000000000000000000000000000000000000000000000000000dead'
                      END AS src_token,
                      CASE
                        WHEN balancerV2Selector = '0x52bbbe29' THEN substr(try_cast(data as varchar), 651, 64)
                        WHEN balancerV2Selector = '0x945bcec9' THEN (
                        --- pick src_token from data[assetOffset + 1]
                          sData[
                            (
                              /* start of assetsOffset */
                              try_cast(
                                varbinary_to_uint256 (from_hex(sData[3])) as integer
                                ) / 32 + 1
                              /* end of assetsOffset */
                            ) + 1
                          ]
                        )
                        ELSE '000000000000000000000000000000000000000000000000000000000000dead'
                      END as dest_token,{% endif %}
                      try_cast(
                        JSON_EXTRACT_SCALAR(balancerData, '$.from_amount') as UINT256
                      ) AS from_amount,
                      try_cast(
                        JSON_EXTRACT_SCALAR(balancerData, '$.to_amount') as UINT256
                      ) AS to_amount,
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
                            when try_cast(src_token as uint256) = uint256 '0' then '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                            else try_cast(
                              from_hex(regexp_replace(src_token, '^(0x)?(00){12}')) as varchar
                            ) -- shrink hex to get address format (bytes20)
                          end as src_token,
                          case
                            when try_cast(dest_token as uint256) = uint256 '0' then '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                            else try_cast(
                              from_hex(regexp_replace(dest_token, '^(0x)?(00){12}')) as varchar
                            ) -- shrink hex to get address format (bytes20)
                          end as dest_token,
                          from_amount,
                          to_amount,
                          try_cast(
                            JSON_EXTRACT_SCALAR(balancerData, '$.quoted_amount') as uint256
                          ) AS quoted_amount,
                          output_received_amount,
                          JSON_EXTRACT_SCALAR(balancerData, '$.metadata') AS metadata,
                          try_cast(
                            from_hex(regexp_replace(beneficiary, '^(0x)?(00){12}')) as varchar
                            -- shrink hex to get address format (bytes20)
                          ) as beneficiary,
                          partnerAndFee,
                          output_partner_share,
                          output_paraswap_share,
                          '{{ method }}' as method{% if inOrOut == 'out' %},
                          output_spent_amount as spent_amount{% endif %}
            from
              {{tableOuter}}
{% endmacro %}
