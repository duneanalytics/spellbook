{{config(
        tags = ['static'],
        schema = 'cex_algorand',
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["algorand"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    ('algorand', 'MTCEM5YJJSYGW2RCXYXGE4SXLSPUUEJKQAWG2GUX6CNN72KQ3XPJCM6NOI', 'Binance', 'Binance 1', 'hildobby', date '2024-04-20')
    , ('algorand', 'QYXDGS2XJJT7QNR6EJ2YHNZFONU6ROFM6BKTBNVT63ZXQ5OC6IYSPNDJ4U', 'Binance', 'Binance 2', 'hildobby', date '2024-04-20')
    , ('algorand', 'JDQ7EW3VY2ZHK4DKUHMNP35XLFPRJBND6M7SZ7W5RCFDNYAA47OC5IS62I', 'Bitfinex', 'Bitfinex 1', 'hildobby', date '2024-04-20')
    , ('algorand', 'UGYKOYFAEB6373DVH6ME6BFVHNG6BMWTYU6VZBMFFI2YQ72QACBSZLNYZ4', 'Bitfinex', 'Bitfinex 2', 'hildobby', date '2024-04-20')
    , ('algorand', 'NWPUVAIOBZHBVLCCYGERFGQ3ZRQPSJ7R6UB3NXFBF552BGYAGX4I5TCVCY', 'Bitfinex', 'Bitfinex 3', 'hildobby', date '2024-04-20')
    , ('algorand', 'J4AEINCSSLDA7LNBNWM4ZXFCTLTOZT5LG3F5BLMFPJYGFWVCMU37EZI2AM', 'HTX', 'HTX 1', 'hildobby', date '2024-04-20')
    , ('algorand', '2X2GV36S66B64URLMRZ4O4IGLWSM5MEKIE6J5VREIZC62GVKCSH25IG4PM', 'KuCoin', 'KuCoin 1', 'hildobby', date '2024-04-20')
    , ('algorand', 'IMGMVBZEPMM36AIMWI7FZHG2G44KEESC5ALZHWX7B7SBNBDY6Z7COYMO6U', 'KuCoin', 'KuCoin 2', 'hildobby', date '2024-04-20')
    , ('algorand', 'NDVDIGWEP77WQDDU5M6F7AAS77AOFXLML7DNNPUEVLQMKTIFHYTTMAG6OU', 'KuCoin', 'KuCoin 3', 'hildobby', date '2024-04-20')
    , ('algorand', 'WBI5LT2BQ7FFYBXW2PEDVB6KBX2F3C77WXBJ2FPVERBXXBUV6SC7XXPGWM', 'KuCoin', 'KuCoin 4', 'hildobby', date '2024-04-20')
    , ('algorand', 'YXDKDH5XHXL6OYMH2HYCJCXOZWPOBEUNK5ICFVJRFW3JVQXZ6HQ6QPVQVA', 'KuCoin', 'KuCoin 5', 'hildobby', date '2024-04-20')
    , ('algorand', 'FQQQS3UJFSNYCII2KE5XSCUB5ZIV2HUFVQ22QYLGI3ONFTPOFMAF5HLLZE', 'LAToken', 'LAToken 1', 'hildobby', date '2024-04-20')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)
