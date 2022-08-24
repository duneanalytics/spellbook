CREATE OR REPLACE VIEW dodoex.view_markets_bsc (market_contract_address, base_token_symbol, quote_token_symbol, base_token_address, quote_token_address) AS VALUES
('\x6064dbd0ff10bfed5a797807042e9f63f18cfe10'::bytea, 'USDC'::text, 'BUSD'::text, '\x8ac76a51cc950d9822d68b83fe1ad97b32cd580d'::bytea, '\xe9e7cea3dedca5984780bafc599bd69add087d56'::bytea),
('\xbe60d4c4250438344bec816ec2dec99925deb4c7'::bytea, 'BUSD'::text, 'USDT'::text, '\xe9e7cea3dedca5984780bafc599bd69add087d56'::bytea, '\x55d398326f99059ff775485246999027b3197955'::bytea),
('\x5bdcf4962fded6b7156e710400f4c4c031f600dc'::bytea, 'KOGE'::text, 'WBNB'::text, '\xe6df05ce8c8301223373cf5b969afcb1498c5528'::bytea, '\xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'::bytea),
('\x8d078451a63d118bacc9cc46698cc416f81c93e2'::bytea, 'LINK'::text, 'BUSD'::text, '\xf8a0bf9cf54bb92f17374d9e9a321e6a111a51bd'::bytea, '\xe9e7cea3dedca5984780bafc599bd69add087d56'::bytea),
('\x327134de48fcdd75320f4c32498d1980470249ae'::bytea, 'WBNB'::text, 'BUSD'::text, '\xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'::bytea, '\xe9e7cea3dedca5984780bafc599bd69add087d56'::bytea),
('\xb76f0218d0ccb31a28d0c35c62411a008a106a36'::bytea, 'KOGE'::text, 'WBNB'::text, '\xe6df05ce8c8301223373cf5b969afcb1498c5528'::bytea, '\xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'::bytea),
('\x4bcc41bfdb5668508d12d020046eed82c1b8b6d2'::bytea, 'KOGE'::text, 'WBNB'::text, '\xe6df05ce8c8301223373cf5b969afcb1498c5528'::bytea, '\xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'::bytea),
('\x89e5015ff12e4536691abfe5f115b1cb37a35465'::bytea, 'ETH'::text, 'BUSD'::text, '\x2170ed0880ac9a755fd29b2688956bd959f933f8'::bytea, '\xe9e7cea3dedca5984780bafc599bd69add087d56'::bytea),
('\xb1327b6402ddba34584ab59fbe8ac7cbf43f6353'::bytea, 'DOT'::text, 'BUSD'::text, '\x7083609fce4d1d8dc0c979aab8c869ea2c873402'::bytea, '\xe9e7cea3dedca5984780bafc599bd69add087d56'::bytea),
('\xc64a1d5c819b3c9113ce3db32b66d5d2b05b4cef'::bytea, 'BTCB'::text, 'BUSD'::text, '\x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c'::bytea, '\xe9e7cea3dedca5984780bafc599bd69add087d56'::bytea),
('\x82aff931d74f0645ce80e8f419b94c8f93952686'::bytea, 'WBNB'::text, 'USDT'::text, '\xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'::bytea, '\x55d398326f99059ff775485246999027b3197955'::bytea)
;

