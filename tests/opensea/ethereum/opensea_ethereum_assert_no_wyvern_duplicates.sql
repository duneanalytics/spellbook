WITH unit_tests as
(
  select
    token_id
    ,count(*) count
  from opensea.trades
  where
    block_number = 13914749
    tx_hash = '0xa54e6fc7bd730c877902af79142ea2021f4bf865dad8ee79109037f1484366aa'
  group by 1
)
select * from unit_test where token_in = 0 and count > 41
