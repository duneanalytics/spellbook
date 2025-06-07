{{
    config(
        schema = 'tokens_bnb'
        ,alias = 'bep20'
        ,tags = ['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address
    , symbol
    , decimals
FROM
(
    VALUES
    -- tokens which do not exist in automated tokens.bep20
    (0x0aee8703d34dd9ae107386d3eff22ae75dd616d1, 'SLICE', 18)
    , (0xfa36810552aaf762942bdddb958a5bca2acc8476, 'anySGEM', 18)
    , (0xa83ebae74ac8f256dd3fdfea409b98eb44c7c52b, 'anyvUNIT', 18)
    , (0x14943e8bdf07049470b6a001f9b09ddad0c0065b, 'anyFLOKIFM', 9)
    , (0x4164a997b0b9a3de5e8ed35e59b015f9e892b7a9, 'DEGEN', 18)
    , (0xf5c8054efc6acd25f31a17963462b90e82fdecad, 'MDA', 18)
    , (0x72ee88374d8087eeb069510a79c7220d90dc4779, 'ENK', 18)
    , (0x1024fb39d9938d3b028e940b95c84a326cd57a58, 'anyPX', 18)
    , (0x6ff0609046a38d76bd40c5863b4d1a2dce687f73, 'MUSIC', 18)
    , (0x250c30a63374a613ed14a0e4d7bd7d14abab8fbf, 'anyXCUR', 8)
    , (0xf99fc9f2d4b9f6cc4d6e56b13c5ae7e0030fb406, 'anywBAN', 18)
    , (0x55af5865807b196bd0197e0902746f31fbccfa58, 'SHO', 18)
    , (0xb247a33e8201586a06f564b603759df7990e744d, 'anyHDO', 9)
    , (0x0aaef3db5d2847b231468658001c278ee892b7e8, 'BAB', 18)
    , (0x8b3cc638431b34014fa3de174a25b18b364a7cc5, 'WINE', 18)
    , (0x0d66f9aa410ca289105355b4940e7551fdce2c14, 'UCG', 0)
    , (0x0fc22a684ee0f37efa8f7dd12f48f62c17ee0c92, 'MIL', 8)
    , (0x9611579c926294b0e29e5371a81a3e463650be17, 'BCP', 18)
    , (0x09a15e2ecc0306046826d95696c183d533c228df, 'BXR', 18)
    , (0xf0a8a368989e1936d5a94d5fb1cb2cd94a7d8e2d, 'anyAUR', 18)
    , (0xbfdcce6ab5533a8b285a86116c957808e118c3b6, 'MYM', 18)
    , (0xe5cf1558a1470cb5c166c2e8651ed0f3c5fb8f42, 'anyDOPE', 18)
    , (0xc36d14c0be40dc9ab88760d2112cc5a69ff8b3c7, 'any$FORWARD', 18)
    , (0x3cbc101e0ab2ef6fed8eb9bea24e7ec97bfca98f, 'LMY', 18)
    , (0x1e06229b037ec6bfeb48fc4d3b473e6c91bb99b7, 'anyMOR', 18)
    , (0xc1be9a4d5d45beeacae296a7bd5fadbfc14602c4, 'VID', 18)
    , (0x0baf6f41514812880212b2e2bb59fe3ca147d6a2, 'CREAM', 18)
    , (0x70d6b3cfaabd6c0eb6b99b80c6540fa754a72c40, 'JUST', 10)
    , (0xec82f8d35ffe8da987b97c0bf12921ec57f23d3c, 'RTKN', 18)
    , (0x3878110877052425282a7d4a712b1d8a45ad04cb, 'MEGASHIB', 9)
    , (0x96a70751fd5bf2b18091b99ef7689b9a3a90920e, 'WSSQUID', 18)
    , (0x26414054e097156c58b0294a9189a34c06bb0ecb, 'anySMCW', 18)
    , (0x85f0d0c799f1ddd9c255f074ef0fa6abe5f32eb9, 'anySLEEPEE', 18)
    , (0x981e360161a37de663f232e6c800fc6a56fb536a, 'URQA', 18)
    , (0x9f080368661261ee5d03bbd2b2f09d5489225e2c, 'STRIP', 18)
    , (0x2c591c8078ea432b7affb04957c89fedc5e854b5, 'CCC', 18)
    , (0xacb5f391f51e15418845bec74781bdd1cbe0fd89, 'MYTH', 18)
    , (0x05134427ca04fe0712b29fb50c4d573f63e5cb22, 'vBABY', 18)
    , (0x89c90e480a39fbe3886bb5bd53ba5b1acc69d4fb, 'IM', 18)
    , (0xce1ab3678e56b4a03d14a66c7ee23c055fdaea16, 'anyGBOND', 18)
    , (0x56f3878c54ad3cec18a380e788c5135bf617f5d6, 'INCH', 9)
    , (0x1ad9fb7a447e8bad3a9cf387046335f29ad414eb, 'any$DEWO', 18)
    , (0xcc41555e193d56b2b5c07db69418d90dfaf20c08, 'anyONX', 18)
    , (0xf9a7bbfdc269dc9d338b97670d3e8b6ec8747618, 'RLC', 9)
    , (0xa0eda2d19211d108772711a3698c69f79673aec8, 'anyHOD', 18)
    , (0x068d05bf6a1d907456b7769458902892329b02b6, 'anySWAP', 18)
    , (0x323a07f929f7c4db7631866af151248ae3912d98, 'LIX', 18)
    , (0x6cee1e8763589d77746e7a0ec84f9815402facd7, 'anySELECT', 18)
    , (0x72449ed79841981b19d4552861007a63da3963fe, 'ETHIX', 18)
    , (0xfe4c11f7db2dde18f2952c52f37abbeb120ab728, 'anyWCHI', 8)
    , (0x56fb3fd352e5875c6f2771ca86f3fd361cc0d93e, 'BITX', 18)
    , (0x5c9565950d2124d6e8387e2b3e9e0cd17fd8f6bd, 'MSU', 18)
    , (0x2a0da514b2281b12a9b93ff1b5ed738a91e0da22, 'SNO TOKEN', 18)
    , (0xa802e06cf47a4bc6a8c99f525be1400d6cc29301, 'anySGEM', 18)
    , (0xcc1f1cbf22293cd906f9e7c4419fbbcde9bd8148, 'anyHER', 9)
    , (0xe6d19cfa419fc81029b11b335ba5c53a0aa6e37f, 'DEMON', 18)
    , (0xa688223dffd18097edd29e1ace08e6b6940b96d6, 'COC', 8)
    , (0xf0c918b2a27746afc863d32a9a07b5cad3a0ef42, 'GBOND', 18)
    , (0x749fec660a245f8b4c3b9bbc8a1ebf1c22863c8e, 'PARETO', 18)
    , (0x8ad96050318043166114884b59e2fc82210273b3, 'NEX', 8)
    , (0xfe77d71baf7a6cdabbd63a2ad1e0adb68ca64c06, 'POWER', 18)
)
AS temp_table (contract_address, symbol, decimals)