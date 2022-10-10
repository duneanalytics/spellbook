{{config(alias='validators_bnb',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')}}

SELECT blockchain, address, name, category, contributor, source, created_at, updated_at
FROM (VALUES
    -- Binance, Source: https://etherscan.io/accounts/label/binance
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()),
    (array('bnb'),'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance 1', 'validators', 'soispoke', 'static', timestamp('2022-10-07'), now()))
    AS x (blockchain, address, name, category, contributor, source, created_at, updated_at)