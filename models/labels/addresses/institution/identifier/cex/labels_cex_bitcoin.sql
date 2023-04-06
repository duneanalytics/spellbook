{{config(alias='cex_bitcoin',
        post_hook='{{ expose_spells(\'["bitcoin"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
    -- Binance, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/binance_address.txt
    ('bitcoin','34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo', 'Binance 1', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','3LYJfcfHPXYJreMsASk2jkn69LWEYKzexb', 'Binance 2', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','3M219KR5vEneNb47ewrPfWyb5jQ2DjxRP6', 'Binance 3', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','3JJmF63ifcamPLiAmLgG96RA599yNtY3EQ', 'Binance 4', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','3HdGoUTbcztBnS7UzY4vSPYhwr424CiWAA', 'Binance 5', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h', 'Binance 6', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    -- Bitfinex, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitfinex_address.txt
    , ('bitcoin','1Kr6QSydW9bFQG1mXiPNNu6WpJGmUa9i1g', 'Bitfinex 1', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','3JZq4atUahhuA9rLhXLMhhTo133J9rF97j', 'Bitfinex 2', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97', 'Bitfinex 3', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    -- Bybit, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bybit_address.txt
    , ('bitcoin','bc1q2qqqt87kh33s0er58akh7v9cwjgd83z5smh9rp', 'Bybit 1', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','bc1q9w4g79ndel72lygvwtqzem67z6uqv4yncvqjz3yn8my9swnwflxsutg4cx', 'Bybit 2', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','bc1qjysjfd9t9aspttpjqzv68k0ydpe7pvyd5vlyn37868473lell5tqkz456m', 'Bybit 3', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','1grwdkr33gt6luumniyjkegjtlhsl5kmqc', 'Bybit 4', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    -- Crypto.com, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/crypto-com_address.txt
    , ('bitcoin','bc1qpy4jwethqenp4r7hqls660wy8287vw0my32lmy', 'Crypto.com 1', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','3lhhdlbvwbzchnqv8dn4ndkfcnycg1fq', 'Crypto.com 2', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','3qsqsaxq4rqrnvh5pew55hf3f9peyb7rvq', 'Crypto.com 3', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','bc1qr4dl5wa7kl8yu792dceg9z5knl2gkn220lk7a9', 'Crypto.com 4', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','bc1q4c8n5t00jmj8temxdgcc3t32nkg2wjwz24lywv', 'Crypto.com 5', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','14m3sd9hccfjw4lymahjckmabaxtk4daqw', 'Crypto.com 6', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    -- Derebit, Source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/deribit_address.txt
    , ('bitcoin','bc1qa3phj5uhnuauk6r62cku6r6fl9rawqx4n6d690', 'Derebit 1', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','bc1qtq5zfllw9fs9w6stnfgalf9v59fgrcxxyawuvm', 'Derebit 2', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','1mdq7zylw6okichbfi_ddz3aak59byc6ct', 'Derebit 3', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','1932ekraq3ad9menbh14wfqbnrlakeept', 'Derebit 4', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','13jj1nxdex5fmsdeyghilok8rf2aygq1cx', 'Derebit 5', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','1mdrcezfi_jdvs6evswux6bwbo_px8if5u3', 'Derebit 6', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','35whp4hid61pey_h4tuhnunw_rj2gtnb41lo', 'Derebit 7', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin','34zhv8_dd6uuceuabydwpki6f4qkqnt_euf', 'Derebit 8', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    -- Gate.io, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/gate-io_address.txt
    , ('bitcoin', '14kmvhqrwrnehbrskbysj4qhgjemdts3sf', 'Gate.io 1', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '162bzzt2hjfv5gm3zmwf_wf_hj_jctmd6rhw', 'Gate.io 2', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1ekkgxr7dtbzbrkfkoe6yep4gj4gzme_kvw', 'Gate.io 3', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1g47msr3oanxmafvr8uc4pzv7fea_zo3r9', 'Gate.io 4', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1hp_ed69tpksaea_wp_y3udt1dtcvcucuoh2y', 'Gate.io 5', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '3hrodxv8hmzk_rtasf_bffrgedkpru8fgy6m', 'Gate.io 6', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    -- Huobi, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/gate-io_address.txt
    , ('bitcoin', '12qtdzhx6f77aq74cpczgsy47varwyjvd8', 'Huobi 1', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '143glvwyuojxawzrxqu_rkp_vntkhmr415b', 'Huobi 2', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1kvpu_cfhftkzj67z_uega_muayey7qni7ppj', 'Huobi 3', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '14xksv8tt6tt8p8mfdqzg_nf8wtn5ernu5d', 'Huobi 4', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    -- KuCoin, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/kucoin_address.txt
    , ('bitcoin', '38fJPq4dYGPoJizEUGCL9yWkqg73cJmC2n', 'KuCoin 1', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', 'bc1q080rkmk3kj86pxvf5nkxecdrw6nrx3zzy9xl7q', 'KuCoin 2', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', 'bc1q8yja3gw33ngd8aunmfr4hj820adc9nlsv0syvz', 'KuCoin 3', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', 'bc1qgrxsrmrhsapvh9addyx6sh8j4rw0sn9xtur9uq', 'KuCoin 4', 'institution', 'hildobby', 'static', timestamp('2022-11-14'), now(), 'cex_bitcoin', 'identifier')
    -- OKX, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/okx_address.txt
    , ('bitcoin', '13jTtHxBPFwZkaCdm6BwJMMJkqvTpBZccw', 'OKX 1', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '13rCGm4Z3PDeYwo5a7GTT4jFYnRFBZbKr1', 'OKX 2', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '14kHu26yWkVD8qAnBfcFXHXxgquNoSpKum', 'OKX 3', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '15Exz1BAVan4Eweagy1rcPJnfyc6KJ4GvL', 'OKX 4', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '162z6mSSHzfTqb2Sn3NUk5r1Y2oGoCMCoM', 'OKX 5', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '16rF2zwSJ9goQ9fZfYoti5LsUqqegb5RnA', 'OKX 6', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '178E8tYZ5WJ6PpADdpmmZd67Se7uPhJCLX', 'OKX 7', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '18QUDxjDZAqAJorr4jkSEWHUDGLBF9uRCc', 'OKX 8', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1AumBaQDRaCC3cKKQVRHeyvoSPWNdDzsKP', 'OKX 9', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1BsdDaJtgFZrLfzEXvh6cD4VhtHHSHhMea', 'OKX 10', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1CE8chGD6Nu8qjcDF2uR1wMKyoWb8Kyxwz', 'OKX 11', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1CY7fykRLWXeSbKB885Kr4KjQxmDdvW923', 'OKX 12', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1DVTB9YKi4KNjyEbAHPp17T8R1Pp17nSmA', 'OKX 13', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1DcT5Wij5tfb3oVViF8mA8p4WrG98ahZPT', 'OKX 14', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1DnHx95d2t5URq2SYvVk6kxGryvTEbTnTs', 'OKX 15', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1FTgXfXZRxMQcKSNeuFvWYVPsNgurTJ7BZ', 'OKX 16', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1FY6RL8Ju9b6CGsHTK68yYEcnzUasufyCe', 'OKX 17', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1FfgXrQUjX5nQ4zsiLBWjvFwW61jQHCqn', 'OKX 18', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1JQULE6yHr9UaitLr4wahTwJN7DaMX7W1Z', 'OKX 19', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1Lj2mCPJYbbC2X6oYwV6sXnE8CZ4heK5UD', 'OKX 20', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1LnoZawVFFQihU8d8ntxLMpYheZUfyeVAK', 'OKX 21', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1M6E6vPaYsuCb34mDNS2aepu2aJyL6xBG4', 'OKX 22', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', '1MbNM3jwxMjRzeA9xyHbMyePN68MY4Jxb', 'OKX 23', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', 'bc1quhruqrghgcca950rvhtrg7cpd7u8k6svpzgzmrjy8xyukacl5lkq0r8l2d', 'OKX 24', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    , ('bitcoin', 'bc1qphk6rkypc8q64xesgy67l8n5780f2kuh286x9j5a5vje4p6mtgtqkzd2s8', 'OKX 25', 'institution', 'hildobby', 'static', timestamp('2023-04-06'), now(), 'cex_bitcoin', 'identifier')
    ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)
