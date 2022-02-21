BEGIN;
DROP VIEW IF EXISTS qidao.view_lp_basic_info CASCADE;

CREATE VIEW qidao.view_lp_basic_info(lp_contract_address, dex_name, lp_name, token_a_symbol, token_b_symbol, token_a_decimals, token_b_decimals) AS VALUES
('\x9a8b2601760814019b7e6ee0052e25f1c623d1e6'::bytea, 'QuickSwap'::text, 'MATIC-QI'::text, 'MATIC'::text, 'QI'::text, 18::integer, 18::integer),
('\x7afcf11f3e2f01e71b7cc6b8b5e707e42e6ea397'::bytea, 'QuickSwap'::text, 'QI-MAI'::text, 'QI'::text, 'MIMATIC'::text, 18::integer, 18::integer),
('\x160532d2536175d65c03b97b0630a9802c274dad'::bytea, 'QuickSwap'::text, 'USDC-MAI'::text, 'USDC'::text, 'MIMATIC', 6::integer, 18::integer),
('\x447646e84498552e62eCF097Cc305eaBFFF09308'::bytea, 'CRV'::text, 'MAI+3Pool3CRV-f'::text, 'MIMATIC'::text, 'am3CRV'::text, 18::integer, 18::integer
),
('\xa3fa99a148fa48d14ed51d610c367c61876997f1'::bytea, NULL::text, 'MAI'::text, 'MIMATIC'::text, NULL::text, 18::integer, NULL::integer); -- there still are MAI tokens in pool

COMMIT;