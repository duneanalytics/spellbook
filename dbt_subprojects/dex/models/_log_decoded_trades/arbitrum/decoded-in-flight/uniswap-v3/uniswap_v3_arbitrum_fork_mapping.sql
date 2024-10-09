-- this should probably live somewhere else, just for testing purposes for now

{{ config(
    schema = 'uniswap_v3_arbitrum',
    alias = 'fork_mapping',
    tags = ['static'],
    unique_key = ['factory_address'])
}}

SELECT factory_address, project_name
FROM
(VALUES
    (0x1F98431c8aD98523631AE4a59f267346ea31F984, 'uniswap_v3')
    
) AS t (factory_address, project_name)


   