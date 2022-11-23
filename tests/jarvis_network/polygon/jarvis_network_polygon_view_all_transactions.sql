with unit_test as (
    select
        case when test.action = actual.action then true else false end                                                      AS action_test,
        case when test.user = actual.user then true else false end                                                          AS user_test,
        case when test.jfiat_token_symbol = actual.jfiat_token_symbol then true else false end                              AS jfiat_token_test,
        case when ABS(test.jfiat_token_amount - actual.jfiat_token_amount) < 0.001 then true else false end                 AS jfiat_token_amount_test,
        case when test.collateral_symbol = actual.collateral_symbol then true else false end                                AS collateral_token_test,
        case when ABS(test.collateral_token_amount - actual.collateral_token_amount) < 0.001 then true else false end       AS collateral_token_amount_test,
        case when ABS(test.net_collateral_amount - actual.net_collateral_amount) < 0.001 then true else false end           AS net_collateral_amount_test
    from       {{ref ('jarvis_network_polygon_all_transactions')}} actual
    INNER JOIN {{ref ('jarvis_network_polygon_view_transactions_seed')}} test 
    ON (actual.evt_tx_hash = test.evt_tx_hash AND actual.evt_index = test.evt_index)

)

select * from unit_test
where (action_test = false                  OR 
       user_test   = false                  OR
       jfiat_token_test = false             OR 
       jfiat_token_amount_test = false      OR 
       collateral_token_test = false        OR 
       collateral_token_amount_test = false OR 
       net_collateral_amount_test = false)
