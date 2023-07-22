{{config(
      tags = ['dunesql','static'],
      alias = alias('relayers'),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "addresses",
                                    \'["msilb7"]\') }}')}}


SELECT 
wallet_address, project_name

FROM (

     (0xAd003006ceB0934012c289D1CfDb1db915998F74, 'Debank')
    ,(0x65bf36d6499a504100eb504f0719271f5c4174ec, 'Worldcoin')
    ,(0xabe494eaa4ed80de8583c49183e9cbdadbc3b954, 'Worldcoin')
    ,(0xb54a5205ee454f48ddfc23ca26a3836ba3dacc07, 'Worldcoin')
    ,(0x4399fa85585f90da110d5ba150ff96c763bc0aba, 'Worldcoin')
    ,(0xd8f7d2d62514895475afe0c7d75f31390dd40de4, 'Worldcoin')
) a (wallet_address, project_name)