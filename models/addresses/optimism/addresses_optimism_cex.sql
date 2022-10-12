{{config(alias='cex',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "addresses",
                                    \'["msilb7"]\') }}')}}

SELECT lower(address) as address, cex_name, distinct_name
FROM (VALUES
     ("0x88880809d6345119ccabe8a9015e4b1309456990","Juno","Juno 1")
    ,("0x5122e9aa635c13afd2fc31de3953e0896bac7ab4","Coinbase","Coinbase 1")
    ,("0xf491d040110384dbcf7f241ffe2a546513fd873d","Coinbase","Coinbase 2")
    ,("0xd839c179a4606f46abd7a757f7bb77d7593ae249","Coinbase","Coinbase 3")
    ,("0xc8373edfad6d5c5f600b6b2507f78431c5271ff5","Coinbase","Coinbase 4")
    ,("0xdfd76bbfeb9eb8322f3696d3567e03f894c40d6c","Coinbase","Coinbase 5")
    ,("0xebb8ea128bbdff9a1780a4902a9380022371d466","KuCoin","KuCoin 1")
    ,("0xd6216fc19db775df9774a6e33526131da7d19a2c", "KuCoin", "KuCoin 2")
    ,("0xf977814e90da44bfa03b6295a0616a897441acec","Binance","Binance 1")
    ,("0x43c5b1c2be8ef194a509cf93eb1ab3dbd07b97ed","Binance","Binance 2")
    ,("0xacd03d601e5bb1b275bb94076ff46ed9d753435a","Binance","Binance 3")
    ,("0x66f791456b82921cbc3f89a98c24ea21784973a1", "Binance", "Binance 4")
    ,("0xf2de20dbf4b224af77aa4ff446f43318800bd6b4", "Binance", "Binance 5")
    ,("0x7ab33ad1e91ddf6d5edf69a79d5d97a9c49015d4", "Binance", "Binance 6")
    ,("0x4d072a68d0428a9a3054e03ad7ee61c557b537ab", "Binance", "Binance 7")
    ,("0xc3c8e0a39769e2308869f7461364ca48155d1d9e", "Binance", "Binance 8")
    ,("0x1763f1a93815ee6e6bc3c4475d31cc9570716db2", "Binance", "Binance 9")
    ,("0x972bed5493f7e7bdc760265fbb4d8e73ea89e453", "Binance", "Binance 10")
    ,("0x79fafb8ef911804ebedfd35ed888a69cd183f79c", "Binance", "Binance 11")
    ,("0x36b06e0b929f40365eebaa81ef25edfcc624a0df", "Binance", "Binance 12")
    ,("0x1bf7f994cf93c4eaab5f785d712668e2d6fff9d6", "Binance", "Binance 13")
    ,("0xb22ffd456ab4efc3863be8299f4a404d813b92be", "Binance", "Binance 14")
    ,("0xef7fb88f709ac6148c07d070bc71d252e8e13b92", "Binance", "Binance 15")
    ) AS x (address, cex_name, distinct_name)
