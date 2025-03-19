{{
    config(
        schema = 'zeroex',
        alias = 'polygon_settler_addresses',
        materialized = 'table'
    )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for Polygon
{{ zeroex_settler_addresses('polygon') }} 