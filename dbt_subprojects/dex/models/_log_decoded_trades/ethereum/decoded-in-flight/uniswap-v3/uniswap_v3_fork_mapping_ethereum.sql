-- this should probably live somewhere else, just for testing purposes for now

{{ config(
    schema = 'dex_mass_decoding_ethereum',
    alias = 'uniswap_v3_fork_mapping',
    tags = ['static'],
    unique_key = ['factory_address'])
}}

SELECT factory_address, project_name
FROM
(VALUES
    (0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f, 'uniswap_v2')
    , (0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac, 'sushi')
    
) AS t (factory_address, project_name)


   