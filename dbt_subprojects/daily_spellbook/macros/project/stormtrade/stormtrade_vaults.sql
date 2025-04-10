{%- macro stormtrade_ton_vaults()
-%}
{# Contains vaults addresses #}

SELECT vault, vault_token FROM (VALUES 
    (upper('0:33e9e84d7cbefff0d23b395875420e3a1ecb82e241692be89c7ea2bd27716b77'), 'USDT'),
    (upper('0:f29d17a209e2bcc652916d802ba69e23cb366e17afad61f8453343b9ba53ace4'), 'jUSDT'),
    (upper('0:e926764ff3d272c73ddeb836975c5521c025ad68e7919a25094e2de3198805f1'), 'TON'),
    (upper('0:06f3f073c255a49aa6fdcc89abf512638e065908b30e4173fd3d1d01d4f607bd'), 'NOT')
    ) AS T(vault, vault_token)

{%- endmacro -%}