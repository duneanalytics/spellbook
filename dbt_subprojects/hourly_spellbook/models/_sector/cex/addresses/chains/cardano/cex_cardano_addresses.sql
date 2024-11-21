{{config(
        tags = ['static'],
        schema = 'cex_cardano',
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["cardano"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    ('cardano', 'addr1qxrlkh6yh0km5m5n7923syel0yqqvc3pjrnqrzrz3gwpxd70prfqweha', 'Bitfinex', 'Bitfinex 1', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1q9w7x0secwr3uz397nl3zw4wc7w9su22rlc7v54p5q425xjauvlpnsu8', 'Bitstamp', 'Bitstamp 1', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1q9frvl4a0wgmk4e28gu4asyqrd6ezd3wn3e2wdq4h3hn73zjxelt67u3', 'Bitstamp', 'Bitstamp 2', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1qysvm626pgxcwgy2w7fk2ulmw0mc6v3twzes3arns64hk5eqeh545zsd', 'Bitstamp', 'Bitstamp 3', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1qy7f98r5ar2ayyst7lmm90tl630tsetygecz36vq33qmvevngs3w3w3m', 'CoinDCX', 'CoinDCX 1', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1qy5dqjx4mtegqu6kx5gu6upkk96c8haf38eksj5tr8g5dae2z9j36evk', 'CoinDCX', 'CoinDCX 2', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1q90372c396d7w0cw3agseaj8vf4t8m9e3lwwc752jd30hyt4pthja5v6', 'CoinDCX', 'CoinDCX 3', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1qyu9n5wtyufnp6vuhcak38tx4lw37znzkulat7s987xtv86xx265k42g', 'CoinDCX', 'CoinDCX 4', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1q9h690lw5hpd37r4ngmlhf0y8wms394y4dlm0wa6nmann9pm8fwxgvus', 'CoinDCX', 'CoinDCX 5', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1qx47g9mejkukkspsp6jdjdzf2lmektye8w7n0cm9ukxvewdljr2awtjh', 'CoinDCX', 'CoinDCX 6', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1q8m3qp920cljg5g079909xrvhngfln5u9n8xug6jtz44rfsm9sj09fks', 'CoinDCX', 'CoinDCX 7', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1qx824fl32wrgankldpnyt7txz0mel5d9cldfkus4xykl3uzqzu4gt7tq', 'CoinDCX', 'CoinDCX 8', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1q8xeul4sslt5xyeaa02vzgquwagv84t9xcjekauja5cz4wsat2uc2fe4', 'CoinDCX', 'CoinDCX 9', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1q9m8eq44qcqe40yz0jwcuc3vs9t7zr6ukz8ctvfkjma5dmxdlpn5em67', 'CoinDCX', 'CoinDCX 10', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1qyzrghamkktczv3wujrdqt243emzu49zu0z3vxw768kpy0fknd8whx6g', 'CoinDCX', 'CoinDCX 11', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1q9phfjzqhcndne6chkxvtwt209n4335ghy0389mp5jfh3gyhry659z5g', 'NBX', 'NBX 1', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1qywum3fvtfrw4t52xk6y2ls9dsgkgwk759fxrnpae7f4q5d3uk2aw97y', 'NBX', 'NBX 2', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1qxqut96hxv5zxmhcgspmnq9tuaf6xglvq6tdv8jm5zltatv5hnm8tps2', 'Swissborg', 'Swissborg 1', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1q9vrcmu4sr7yrspknu8gwrzgrs6wuh0e6pkk9tyz2clg9llt77jyl742', 'Swissborg', 'Swissborg 2', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1qy9ffv7zmqtmenskcnvsxszhv6zsls8gkl339tc5d2c5davhg2p4nekj', 'Swissborg', 'Swissborg 3', 'hildobby', date '2024-04-20')
    , ('cardano', 'addr1qx2tzwkx4fjg8cg0htw27cje4029cmf2plsm3nws5qyky45njhmrzd25', 'Swissborg', 'Swissborg 4', 'hildobby', date '2024-04-20')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)
