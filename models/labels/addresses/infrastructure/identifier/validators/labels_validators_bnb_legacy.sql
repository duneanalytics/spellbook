{{config(
	tags=['legacy'],
	alias = alias('validators_bnb', legacy_model=True),
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
    -- Binance, Source: https://etherscan.io/accounts/label/binance
    ('bnb','0xAAcF6a8119F7e11623b5A43DA638e91F669A130f', 'BNB Validator: Neptune', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x70F657164e5b75689b64B7fd1fA275F334f28e18', 'BNB Validator: BscScan', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x3f349bBaFEc1551819B8be1EfEA2fC46cA749aA1', 'BNB Validator: Legend II', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0xE9AE3261a475a27Bb1028f140bc2a7c843318afD', 'BNB Validator: HashQuark', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x2D4C407BBe49438ED859fe965b140dcF1aaB71a9', 'BNB Validator: NodeReal', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0xee226379dB83CfFC681495730c11fDDE79BA4c0C', 'BNB Validator: InfStones', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x72b61c6014342d914470eC7aC2975bE345796c2b', 'BNB Validator: BNB48 Club', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x7AE2F5B9e386cd1B50A4550696D957cB4900f03a', 'BNB Validator: Fuji', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x685B1ded8013785d6623CC18D214320b6Bb64759', 'BNB Validator: Namelix', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0xea0A6E3c511bbD10f4519EcE37Dc24887e11b55d', 'BNB Validator: Defibit', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0xBe807Dddb074639cD9fA61b47676c064fc50D62C', 'BNB Validator: Certik', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0xa6f79B60359f141df90A0C745125B131cAAfFD12', 'BNB Validator: Avengers', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x9F8cCdaFCc39F3c7D6EBf637c9151673CBc36b88', 'BNB Validator: Ankr', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x295e26495CEF6F69dFA69911d9D8e4F3bBadB89B', 'BNB Validator: Legend', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x8b6C8fd93d6F4CeA42Bbb345DBc6F0DFdb5bEc73', 'BNB Validator: Legend III', 'infrastructure','soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x2465176C461AfB316ebc773C61fAEe85A6515DAA', 'BNB Validator: TW Staking', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0xe2d3A739EFFCd3A99387d015E260eEFAc72EBea1', 'BNB Validator: MathWallet', 'infrastructure','soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0xEF0274E31810C9Df02F98FAFDe0f841F4E66a1Cd', 'BNB Validator: Tranchess', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x4396e28197653d0C244D95f8C1E57da902A72b4e', 'BNB Validator: Alps', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0xac0E15a038eedfc68ba3C35c73feD5bE4A07afB5', 'BNB Validator: Bison Trails', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0xce2FD7544e0B2Cc94692d4A704deBEf7bcB61328', 'BNB Validator: Pexmons', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x4430b3230294D12c6AB2aAC5C2cd68E80B16b581', 'BNB Validator: Ciscox', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0xB8f7166496996A7da21cF1f1b04d9B3E26a3d077', 'BNB Validator: Coinlix', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x2a7cdd959bFe8D9487B2a43B33565295a698F7e2', 'BNB Validator: Sigm8', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'),
    ('bnb','0x6488Aa4D1955Ee33403f8ccB1d4dE5Fb97C7ade2', 'BNB Validator: Seoraksan', 'infrastructure', 'soispoke', 'static', timestamp('2022-10-07'), now(), 'validators_bnb', 'identifier'))
    AS x (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)