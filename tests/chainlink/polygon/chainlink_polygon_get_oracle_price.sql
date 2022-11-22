with unit_test as (
    select
        case when test.oracle_price = actual.oracle_price then true else false end                                          AS oracle_price_test,
        case when test.proxy_address = actual.proxy_address then true else false end                                        AS proxy_address_test,
        case when test.aggregator_address = actual.aggregator_address then true else false end                              AS aggregator_address_test,
        case when test.underlying_token_address = actual.underlying_token_address then true else false end                  AS underlying_token_address_test,
    from       {{ref ('chainlink_polygon_price_feeds')}} actual
    INNER JOIN {{ref ('chainlink_polygon_get_price')}} test 
    ON (actual.blockchain = test.blockchain AND actual.block_time = test.block_time AND actual.feed_name = test.feed_name)

)

select * from unit_test
where (oracle_price_test = false                OR 
       proxy_address_test   = false             OR
       aggregator_address_test = false          OR 
       underlying_token_address_test = false
      )