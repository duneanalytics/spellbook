{{ config(
       schema = 'ton'
       , alias = 'ton_lst_addresses'
       , materialized = 'view'
   )
 }}

 {#
 The list of LST addresses for TON. Liquid Staking Tokens are special kind and need
 to be handled differently in some cases.
 #}

SELECT 
        UPPER('0:bdf3fa8098d129b54b4f73b5bac5d1e1fd91eb054169c3916dfc8ccd536d1000') AS address, 
        'Tonstakers' AS project
    UNION ALL
    SELECT 
        UPPER('0:92c4664f1ea6b74ed9ce0e031a9fc0843348dfe87a58faea27fcd31e1608caaa'), --bmTON
        'Bemo bmTON' AS project
    UNION ALL
    SELECT 
        UPPER('0:cd872fa7c5816052acdf5332260443faec9aacc8c21cca4d92e7f47034d11892'), --stTON
        'Bemo stTON' AS project
    UNION ALL
    SELECT 
        UPPER('0:aa0ba121449feda569e02b12fa755d24e834a7454aecf4649590b6df742aac8f'),
        'Stakee' AS project
    UNION ALL
    SELECT 
        UPPER('0:cf76af318c0872b58a9f1925fc29c156211782b9fb01f56760d292e56123bf87'),
        'Hipo' AS project
    UNION ALL
    SELECT 
        UPPER('0:6e2215cd36459ff405bd9a234635348efee159db77a37c85ab89e5e07b97fbdf'),
        'KTON' AS project
    UNION ALL
    SELECT 
        UPPER('0:744a8c6e183c79aa356dd0ffdb3c80857967452c1995a291e18e07ecd2afb0b1'),
        'TonWhales' AS project