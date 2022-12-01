with unit_test as (
    select
        case when ABS(test.oracle_price - actual.oracle_price) < 0.00001 then true else false end                           AS oracle_price_test,
        case when test.proxy_address = actual.proxy_address then true else false end                                        AS proxy_address_test,
        case when test.aggregator_address = actual.aggregator_address then true else false end                              AS aggregator_address_test
    from       {{ref ('chainlink_polygon_price_feeds')}} actual
    INNER JOIN {{ref ('chainlink_get_price_seed')}} test 
    ON (actual.blockchain = test.blockchain         AND 
        actual.block_number = test.block_number     AND 
        actual.feed_name = test.feed_name    
       )
)

select * from unit_test
where (
       oracle_price_test = false                 OR 
       proxy_address_test   = false              OR
       aggregator_address_test = false           
      )