{{config(alias = 'dex',
       schema = 'addresses_bnb',
        tags=['static'])
        }}

SELECT address, dex_name, distinct_name
FROM (VALUES
      ('0x1a1ec25dc08e98e5e93f1104b5e5cdd298707d31', 'MetaMask', ''),
      

)
      
      
    ) AS x (address, dex_name, distinct_name)