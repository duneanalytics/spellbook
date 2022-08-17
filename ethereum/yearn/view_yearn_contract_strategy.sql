BEGIN;
DROP VIEW IF EXISTS yearn."view_yearn_contract_strategy";

CREATE VIEW yearn."view_yearn_contract_strategy" AS(
    SELECT
    yct."yvault_contract",
    CASE
        --https://api.yearn.finance/v1/chains/1/vaults/all
        WHEN "yvault_contract" = '\x03403154afc09ce8e44c3b185c82c6ad5f86b9ab'::bytea then '\x7a10be29c4d9073e6b3b6b7d1fb5bcdbeca2aa1f'::bytea
        WHEN "yvault_contract" = '\x0fcdaedfb8a7dfda2e9838564c5a1665d856afdf'::bytea then '\xbcc6abd115a32fc27f7b49f9e17d0bcefdd278ac'::bytea
        WHEN "yvault_contract" = '\x123964ebe096a920dae00fb795ffbfa0c9ff4675'::bytea then '\xd96041c5ec05735d965966bf51faec40f3888f6e'::bytea
        WHEN "yvault_contract" = '\x1b5eb1173d2bf770e50f10410c9a96f7a8eb6e75'::bytea then '\x8c151a8f106bad20a501dc758c19fab28a040759'::bytea
        WHEN "yvault_contract" = '\x2994529c0652d127b7842094103715ec5299bbed'::bytea then '\x112570655b32a8c747845e0215ad139661e66e7f'::bytea
        WHEN "yvault_contract" = '\x2f08119c6f07c006695e079aafc638b8789faf18'::bytea then '\xaa12d6c9d680eafa48d8c1ecba3fcf1753940a12'::bytea
        WHEN "yvault_contract" = '\x37d19d1c4e1fa9dc47bd1ea12f742a0887eda74a'::bytea then '\x4ba03330338172febeb0050be6940c6e7f9c91b0'::bytea
        WHEN "yvault_contract" = '\x39546945695dcb1c037c836925b355262f551f55'::bytea then '\xb21c4d2f7b2f29109ff6243309647a01beb9950a'::bytea
        WHEN "yvault_contract" = '\x46afc2dfbd1ea0c0760cad8262a5838e803a37e5'::bytea then '\xe02363cb1e4e1b77a74faf38f3dbb7d0b70f26d7'::bytea
        WHEN "yvault_contract" = '\x5334e150b938dd2b6bd040d9c4a03cff0ced3765'::bytea then '\x76b29e824c183dbbe4b27fe5d8edf0f926340750'::bytea
        WHEN "yvault_contract" = '\x5533ed0a3b83f70c3c4a1f69ef5546d3d4713e44'::bytea then '\xd7f641697ca4e0e19f6c9cf84989abc293d24f84'::bytea
        WHEN "yvault_contract" = '\x597ad1e0c13bfe8025993d9e79c69e1c0233522e'::bytea then '\x4f2fdebe0df5c92eee77ff902512d725f6dfe65c'::bytea
        WHEN "yvault_contract" = '\x5dbcf33d8c2e976c6b560249878e6f1491bca25c'::bytea then '\x07db4b9b3951094b9e278d336adf46a036295de7'::bytea
        WHEN "yvault_contract" = '\x629c759d1e83efbf63d84eb3868b564d9521c129'::bytea then '\x530da5aef3c8f9ccbc75c97c182d6ee2284b643f'::bytea
        WHEN "yvault_contract" = '\x7f83935ecfe4729c4ea592ab2bc1a32588409797'::bytea then '\x15cfa851403abfbbd6fdb1f6fe0d32f22ddc846a'::bytea
        WHEN "yvault_contract" = '\x7ff566e1d69deff32a7b244ae7276b9f90e9d0f6'::bytea then '\x6d6c1ad13a5000148aa087e7cbfb53d402c81341'::bytea
        WHEN "yvault_contract" = '\x8e6741b456a074f0bc45b8b82a755d4af7e965df'::bytea then '\x33f3f002b8f812f3e087e9245921c8355e777231'::bytea
        WHEN "yvault_contract" = '\x96ea6af74af09522fcb4c28c269c26f59a31ced6'::bytea then '\x153fe8894a76f14bc8c8b02dd81efbb6d24e909f'::bytea
        WHEN "yvault_contract" = '\x98b058b2cbacf5e99bc7012df757ea7cfebd35bc'::bytea then '\xc59601f0cc49baa266891b7fc63d2d5fe097a79d'::bytea
        WHEN "yvault_contract" = '\x9ca85572e6a3ebf24dedd195623f188735a5179f'::bytea then '\x7a10be29c4d9073e6b3b6b7d1fb5bcdbeca2aa1f'::bytea
        WHEN "yvault_contract" = '\xa8b1cb4ed612ee179bdea16cca6ba596321ae52d'::bytea then '\x551f41ad4ebeca4f5d025d2b3082b7ab2383b768'::bytea
        WHEN "yvault_contract" = '\xacd43e627e64355f1861cec6d3a6688b31a6f952'::bytea then '\x2f90c531857a2086669520e772e9d433bbfd5496'::bytea
        WHEN "yvault_contract" = '\xba2e7fed597fd0e3e70f5130bcdbbfe06bb94fe1'::bytea then '\x395f93350d5102b6139abfc84a7d6ee70488797c'::bytea
        WHEN "yvault_contract" = '\xbacb69571323575c6a5a3b4f9eede1dc7d31fbc1'::bytea then '\x8e2057b8fe8e680b48858cdd525ebc9510620621'::bytea
        WHEN "yvault_contract" = '\xcc7e70a958917cce67b4b87a8c30e6297451ae98'::bytea then '\xd42ec70a590c6bc11e9995314fdba45b4f74fabb'::bytea
        WHEN "yvault_contract" = '\xe0db48b4f71752c4bef16de1dbd042b82976b8c7'::bytea then '\x6f1ebf5bbc5e32fffb6b3d237c3564c15134b8cf'::bytea
        WHEN "yvault_contract" = '\xe1237aa7f535b0cc33fd973d66cbf830354d16c7'::bytea then '\x39aff7827b9d0de80d86de295fe62f7818320b76'::bytea
        WHEN "yvault_contract" = '\xe625f5923303f1ce7a43acfefd11fd12f30dbca4'::bytea then '\xbdceae91e10a80dbd7ad5e884c86eae56b075caa'::bytea
        WHEN "yvault_contract" = '\xec0d8d3ed5477106c6d4ea27d90a60e594693c90'::bytea then '\xf4fd9b4dab557dd4c9cf386634d61231d54d03d6'::bytea
        WHEN "yvault_contract" = '\xf6c9e9af314982a4b38366f4abfaa00595c5a6fc'::bytea then '\x3be2717da725f43b7d6c598d8f76aec43e231b99'::bytea
        WHEN "yvault_contract" = '\xfe39ce91437c76178665d64d7a2694b0f6f17fe3'::bytea then '\x406813ff2143d178d1ebccd2357c20a424208912'::bytea
        ELSE "strategy"
    END AS strategy,
    yct."yearn_type"
    FROM
    yearn."view_yearn_contract_tokens" yct 
    LEFT JOIN yearn_v2."yVault_evt_StrategyAdded" ys ON yct."yvault_contract" = ys."contract_address"
    WHERE
    "yearn_type" NOT IN ('iearn_v2','ironbank','woofy')
);
COMMIT;