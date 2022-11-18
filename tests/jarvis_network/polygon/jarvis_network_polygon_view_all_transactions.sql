with unit_test as (
    select
        case when test.action = actual.action then true else false end as action_test,
        case when test.user = actual.user then true else false end as user_test,
        case when test.jfiat_token_symbol = actual.jfiat_token_symbol then true else false end as jfiat_token_test
        case when test.jfiat_token_amount = actual.jfiat_token_amount then true else false end as jfiat_token_amount_test
        case when test.collateral_symbol = actual.collateral_symbol then true else false end as collateral_token_test
        case when test.collateral_token_amount = actual.collateral_token_amount then true else false end as collateral_token_amount_test
        case when test.net_collateral_amount = actual.net_collateral_amount then true else false end as net_collateral_amount_test
        case when ROUND(test.net_amount_usd,4) = ROUND(actual.net_amount_usd,4) then true else false end as net_amount_usd_test
        case when ROUND(test.fee_amount_usd,4) = ROUND(actual.fee_amount_usd,4) then true else false end as fee_amount_usd_test
        case when test.evt_index = actual.evt_index then true else false end as evt_index_test
    from {{ref ('jarvis_network_view_all_transactions')}} as actual
    INNER JOIN {{ref ('jarvis_network_polygon_view_transactions')}} as test
    on actual.evt_tx_hash = test.evt_tx_hash

)

select * from unit_test
where (action_test = false or user_test = false or jfiat_token_test = false or jfiat_token_amount_test = false or collateral_token_test = false or collateral_token_amount_test = false
or net_collateral_amount_test = false or net_amount_usd_test = false or fee_amount_usd_test = false or evt_index_test = false)