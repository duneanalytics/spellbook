 {{
  config(
        tags = ['dunesql','static'],
        schema='uniswap_v3_optimism',
        alias= alias('ovm1_pool_mapping'),
        materialized='table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "uniswap_v3",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}
with ovm1_legacy_pools_raw as (
  select json_parse(json_column) AS json_data
    from (values
        '[
          
          {
            "oldAddress": "0x2e9c575206288f2219409289035facac0b670c2f",
            "newAddress": "0x03af20bdaaffb4cc0a521796a223f7d85e2aac31",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 3000
          },
          {
            "oldAddress": "0x8c505fd76eed0945699265c7c7e5bbf756b7e5ad",
            "newAddress": "0x827f0a2a4376bc26729f398b865f424dc8456841",
            "token0": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 500
          },
          {
            "oldAddress": "0xdd54251a35078ba39e3ad5fb059f9aa243693b9d",
            "newAddress": "0x73b14a78a0d396c521f954532d43fd5ffe385216",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "fee": 3000
          },
          {
            "oldAddress": "0x0ad1af4178e17d7f41dbcdf9d573701bef5eb501",
            "newAddress": "0xdd0c6bae8ad5998c358b823df15a2a4181da1b80",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 3000
          },
          {
            "oldAddress": "0xdf42e37f057c61765fe7204642c4d2e5ff929cfe",
            "newAddress": "0x815ae7bf44dda74ed9274377ed711efc8b567911",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 10000
          },
          {
            "oldAddress": "0x6b952bfbfda057a7f288edaa9f611cd446ddbe22",
            "newAddress": "0x95d9d28606ee55de7667f0f176ebfc3215cfd9c0",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 500
          },
          {
            "oldAddress": "0x0bec645f0373750fe0256ee0e7b06d63eae5e04d",
            "newAddress": "0x2df05e4cdbd758cb1a99a34bb0d767e040d6b078",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 10000
          },
          {
            "oldAddress": "0x6afd8618459729da24ee36978567fb04fe5fd1bd",
            "newAddress": "0x85c31ffa3706d1cce9d525a00f1c7d4a2911754c",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "fee": 500
          },
          {
            "oldAddress": "0xa61dea82c7c3e64a6a80550aacb251eed604b46b",
            "newAddress": "0x37ffd11972128fd624337ebceb167c8c0a5115ff",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "fee": 10000
          },
          {
            "oldAddress": "0xcf438c19332d507326210da527fb9cf792fd3e18",
            "newAddress": "0xc858a329bf053be78d6239c4a4343b8fbd21472b",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 500
          },
          {
            "oldAddress": "0xaddd011cb3b61d0dc4f85c2661cc9bd1bd640067",
            "newAddress": "0xb29a022ff4b37bdfb21e5f1daff4af5a22aa9510",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "fee": 500
          },
          {
            "oldAddress": "0x47516ccba929c607e14dbd02f2ebac1e7960b1f8",
            "newAddress": "0x0392b358ce4547601befa962680bede836606ae2",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "fee": 3000
          },
          {
            "oldAddress": "0x13b2d83ec506b5c770f64ee0f564ff9719c74071",
            "newAddress": "0xfea834a5c47b923add607cc5b96288d18ffb9c3f",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "fee": 10000
          },
          {
            "oldAddress": "0xd20bf925e04933ff79274479009218dedab6657f",
            "newAddress": "0xac721d2e27ca148f505b5106fc95e594c78ace5b",
            "token0": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 10000
          },
          {
            "oldAddress": "0x7678f1e1ed90efec8757af161ab25bf1e8e00238",
            "newAddress": "0xa13514b5444e50067f6e48c386016b211773cf9e",
            "token0": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 10000
          },
          {
            "oldAddress": "0x2816913eda0010af856d323724f521fb702a25a7",
            "newAddress": "0xcf2aebb91fec906f51fc11cd57035a09d8b16965",
            "token0": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 3000
          },
          {
            "oldAddress": "0x2419a5fee0f8e0192869507ed6a301382ad9edda",
            "newAddress": "0xea0f33940eb221aaad9360891cab08ef4f1f0703",
            "token0": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 3000
          },
          {
            "oldAddress": "0x0ccf6bf2df83d250d0f6a636215ef7d19f86dd01",
            "newAddress": "0x703eb589321f3dc7408e9dde01b790e64a9fe4e9",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 10000
          },
          {
            "oldAddress": "0x1135e9ce18373238c77ff602a9b0a579ca86eb8e",
            "newAddress": "0xc22662b904d98e45f89e030201355c3e372cc819",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 3000
          },
          {
            "oldAddress": "0x2f5ccaf670e9c5f4336c127a29fdd4932f238069",
            "newAddress": "0x1aa9b4d9933ff96b2011fddd764240d4a16b7c07",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 500
          },
          {
            "oldAddress": "0x0f641370eb5cb4f0b0d58140d5fb2f97ffcbfce5",
            "newAddress": "0x2459023a29d3b07711b8b916d86aa7e8a14747af",
            "token0": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 500
          },
          {
            "oldAddress": "0xa14e5b3ba5dd981b536e0950390b03972b795018",
            "newAddress": "0xadb35413ec50e0afe41039eac8b930d313e94fa4",
            "token0": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 500
          },
          {
            "oldAddress": "0xbdb9a8279a525bafe9be7efb9b5df79b18eeb23f",
            "newAddress": "0x84eb2c5c23999b3ddc87be10f15ccec5d22c7d97",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "fee": 500
          },
          {
            "oldAddress": "0xa194977b416f082f71a0362041b57208c91ee1c1",
            "newAddress": "0x2e80d5a7b3c613d854ee43243ff09808108561eb",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "fee": 3000
          },
          {
            "oldAddress": "0xc3099d7fd3fc7d4feea11911fbe6eadc94c7c07a",
            "newAddress": "0x3d44cc727fe2f603e4929be164c70edb3b498b5f",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "fee": 10000
          },
          {
            "oldAddress": "0xfe901e734c8c55645731acd4eb0be963d2a85b94",
            "newAddress": "0xa588c9d2884c60b098c5ad028ec2f4a1fab772b5",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "fee": 10000
          },
          {
            "oldAddress": "0xeaa5ba3ef450887e4d5a627700aef3c1a16d4090",
            "newAddress": "0xc53f2be3331926d2f30ee2b10362bb45fdbe7bf6",
            "token0": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "token1": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "fee": 500
          },
          {
            "oldAddress": "0x263312f667279452ad44cda7971fe93f18b6dad4",
            "newAddress": "0x0843e0f56b9e7fdc4fb95fabba22a01ef4088f41",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 3000
          },
          {
            "oldAddress": "0x051580636f94b8b6ba69b879958939d324d8f650",
            "newAddress": "0x8184f5cf4921558c201923ef6d7d5258a6efa31f",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 500
          },
          {
            "oldAddress": "0xbb8a699cbd6b45f7c31dcd14bd6d965ab4293e2c",
            "newAddress": "0x8b057f0ccd9fb78f688472574cf3f9d2322f5454",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 10000
          },
          {
            "oldAddress": "0x98fd8560e184136f482054c19a63e644240e30f4",
            "newAddress": "0x9f08065dfc4817a0a56db7bcab757e86399bc51d",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "fee": 500
          },
          {
            "oldAddress": "0xbeafe824395fff8df37c4814e8de9d455e79cdad",
            "newAddress": "0x7628784d2c5d47fcd5479ba812343b1aabad6484",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "fee": 3000
          },
          {
            "oldAddress": "0xcfe7288e10994555ca97dfa2d0c50e55a4d4dc39",
            "newAddress": "0xceb488e01c8e2e669c40b330bfc1440921c9ebe2",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "fee": 10000
          },
          {
            "oldAddress": "0x4cff717ff0b0a4a3578e8bbb7a5f06d32574238b",
            "newAddress": "0x25e412992634b93a025e2a538c53222a8c62e2d6",
            "token0": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "token1": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "fee": 10000
          },
          {
            "oldAddress": "0x072611197970d6a9e57680f97f177ff947f09139",
            "newAddress": "0xc0f184c6c4832b3ed861bd5b05722792ffa64abd",
            "token0": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "token1": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "fee": 3000
          },
          {
            "oldAddress": "0x380ff418bf1589b46e9660c6b2197b4ce8ae8a12",
            "newAddress": "0xf046d8b7365d8abe5a8f8301c669b4b5284fc21d",
            "token0": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 500
          },
          {
            "oldAddress": "0xac8c823548f13874dcfc76029089de01f4adc1d3",
            "newAddress": "0x1f2390484dfe2d8900bc91c7111d274b7b2d63a1",
            "token0": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 10000
          },
          {
            "oldAddress": "0x8cf0a5fdcaed0956a3221e1dd5219bb14f092595",
            "newAddress": "0xa0959d2dcd9dd56bf080a10cfe29eeb401344e3d",
            "token0": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 10000
          },
          {
            "oldAddress": "0xaf00e47d2fe45befc4540fe02a87cb053e252065",
            "newAddress": "0x30be2fff09fcd820a1d472e646bd233dbd812133",
            "token0": "0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 10000
          },
          {
            "oldAddress": "0x00a0e3cd857a7e5676c901bd349ed1d6afb59fb3",
            "newAddress": "0x3202c46666e774b44ba463eafaa6da9a968a058f",
            "token0": "0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6",
            "token1": "0x4200000000000000000000000000000000000006",
            "fee": 10000
          },
          {
            "oldAddress": "0x90fc3f5f84fb868b7693b1f2690b91f28c1600d0",
            "newAddress": "0x85e8d0fddf559a57aac6404e7695142cd53eb808",
            "token0": "0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6",
            "token1": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "fee": 10000
          },
          {
            "oldAddress": "0xc87adb8ac31434e96b429ced522ed84a2ce707a6",
            "newAddress": "0x22fc5dc36811d15fafde7cc7900ae73a538e59e0",
            "token0": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 10000
          },
          {
            "oldAddress": "0x466fd9d58bdd0e246cbe9112d95d077b81b341af",
            "newAddress": "0xe7ee03b72a89f87d161425e42548bd5492d06679",
            "token0": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 3000
          },
          {
            "oldAddress": "0x7b6467b86878b86163bcb3162d84e34ea5c7389b",
            "newAddress": "0xfe1bd31a79163d6277ab8c2917d7857c225db065",
            "token0": "0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6",
            "token1": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "fee": 3000
          },
          {
            "oldAddress": "0xc24383ba6d156706864a48f50fc01e89c0bf11d7",
            "newAddress": "0xbf595eb9a512b1c274125264aef84a2847158eb3",
            "token0": "0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6",
            "token1": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "fee": 3000
          },
          {
            "oldAddress": "0x93d9dfb5caf591df911b251db4d76cd95f4644b7",
            "newAddress": "0x124657e5bb6afc12a15c439d08fc80070f9a1a1e",
            "token0": "0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 500
          },
          {
            "oldAddress": "0x36c95ae265883c2b19e61997760b110cc05e4a60",
            "newAddress": "0xd6101cda1a51924e249132cbcae82bfcd0a91fbc",
            "token0": "0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 3000
          },
          {
            "oldAddress": "0xd515990647c39c4a0b8c03f811f9b746958a0eec",
            "newAddress": "0x19ea026886cbb7a900ecb2458636d72b5cae223b",
            "token0": "0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6",
            "token1": "0x4200000000000000000000000000000000000006",
            "fee": 3000
          },
          {
            "oldAddress": "0x3b6479c7748eb5b143a3a52d237c0097734b811b",
            "newAddress": "0x5aacc66073cb0c3064353f1441c2e04170b4dbbf",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0xc5Db22719A06418028A40A9B5E9A7c02959D0d08",
            "fee": 3000
          },
          {
            "oldAddress": "0x2d073707207098cc69e8e86c6a3fd12644b8a1b2",
            "newAddress": "0x4284b21e76d1b3977cab8f0032867e00e6eea382",
            "token0": "0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6",
            "token1": "0xc5Db22719A06418028A40A9B5E9A7c02959D0d08",
            "fee": 500
          },
          {
            "oldAddress": "0xac61775e2d76fe18c0a758ddf2ebef63a4d1a3e7",
            "newAddress": "0x5b0e07a0421bd25fb4c45a88fec05b29e83594f6",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0xE405de8F52ba7559f9df3C368500B6E6ae6Cee49",
            "fee": 500
          },
          {
            "oldAddress": "0x015986c7074ec1eeae0387a8baf485fd9d811b7d",
            "newAddress": "0x2f10a1a3e640ad1615cbedf95a1749a4af88cbc0",
            "token0": "0xB548f63D4405466B36C0c0aC3318a22fDcec711a",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 3000
          },
          {
            "oldAddress": "0x4a88e6fa2afad460befd586fc1581f322308c490",
            "newAddress": "0x32846ede08688d10a9da59387707a8fbb0790fa7",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0xab7bAdEF82E9Fe11f6f33f87BC9bC2AA27F2fCB5",
            "fee": 3000
          },
          {
            "oldAddress": "0xccf24898ca659afa8cb6a3bdb8a2e0a2debda12d",
            "newAddress": "0xbdb6371fffc1753b33b87c68c827eb7978670515",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x6fd9d7AD17242c41f7131d257212c54A0e816691",
            "fee": 500
          },
          {
            "oldAddress": "0x856d50c587824f84de481ea706208b03db38f6f2",
            "newAddress": "0x2eee8ed7df992f23d7554b0db8835d483cce901c",
            "token0": "0x298B9B95708152ff6968aafd889c6586e9169f1D",
            "token1": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "fee": 10000
          },
          {
            "oldAddress": "0xe3d8cfc3a0b43d2288b3da41563b1fe0623209de",
            "newAddress": "0x65dc095b35679005229896566928f6852948092b",
            "token0": "0x298B9B95708152ff6968aafd889c6586e9169f1D",
            "token1": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "fee": 500
          },
          {
            "oldAddress": "0x62196490fcf045437e5e4cb49228bbd778b7196d",
            "newAddress": "0x2d6497dd08a1620d386ce708edac50aaec332415",
            "token0": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "token1": "0xE405de8F52ba7559f9df3C368500B6E6ae6Cee49",
            "fee": 10000
          },
          {
            "oldAddress": "0x3b1f6287be238c9b0a4b48d85d2359d58aaa9683",
            "newAddress": "0x039ae8860fbfdf61f654b1a5b55cc3aa753f5842",
            "token0": "0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 3000
          },
          {
            "oldAddress": "0x91e50b184ea237b3da1c005ee5d2a17a904a34c6",
            "newAddress": "0x24342b5d46f69ba05c09becdd00e5324f9f0f7ca",
            "token0": "0x298B9B95708152ff6968aafd889c6586e9169f1D",
            "token1": "0x4200000000000000000000000000000000000006",
            "fee": 10000
          },
          {
            "oldAddress": "0x518767d8ef1acffd978581c16789f8a2803f9bef",
            "newAddress": "0xf0d0e52da1fdde512af299f3d8ea1c5e3bebb96f",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0xe3C332a5DcE0e1d9bC2cC72A68437790570C28a4",
            "fee": 3000
          },
          {
            "oldAddress": "0xc2c0786e85ac9b0b223966d040ebc641fa44225e",
            "newAddress": "0xb589969d38ce76d3d7aa319de7133bc9755fd840",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "fee": 3000
          },
          {
            "oldAddress": "0xa2389a4ee391b4b04ae8dc664664190f3d28f2fe",
            "newAddress": "0x8eda97883a1bc02cf68c6b9fb996e06ed8fdb3e5",
            "token0": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "token1": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "fee": 500
          },
          {
            "oldAddress": "0xf9ca53854d1ac7adb43d9447aa87f17fe1454e31",
            "newAddress": "0x100bdc1431a9b09c61c0efc5776814285f8fb248",
            "token0": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 500
          },
          {
            "oldAddress": "0x4893c5f29301cfd7a6527331f3e06ea82e68a952",
            "newAddress": "0xe229ce1cdbea9983362ca29f0f0b2c70bb2dacdf",
            "token0": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 3000
          },
          {
            "oldAddress": "0xf2e805fe3b15297e1df03b6036d01b32ab8f7998",
            "newAddress": "0xd9b160620447d9a9a6ca90c0450f5490e5219257",
            "token0": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 3000
          },
          {
            "oldAddress": "0x6663eea65669978481bae55814cbc496acd50352",
            "newAddress": "0x1179b19438a622fe36be5f9c073b700420384397",
            "token0": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 10000
          },
          {
            "oldAddress": "0x7bbc5726e6c2640ed0f0fda1546dc232dc5db89c",
            "newAddress": "0xf3f3433c3a97f70349c138ada81da4d3554982db",
            "token0": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 500
          },
          {
            "oldAddress": "0x0d4294ae819ff83a4e2a99db8d06cdd025c19218",
            "newAddress": "0x85149247691df622eaf1a8bd0cafd40bc45154a9",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "fee": 500
          },
          {
            "oldAddress": "0x9e845c705aba9a2ca6e97c2423797e18a98d34c0",
            "newAddress": "0x6fa1ea0ccbbe9b2ad52440c88a47b5d73cd9a731",
            "token0": "0x298B9B95708152ff6968aafd889c6586e9169f1D",
            "token1": "0x4200000000000000000000000000000000000006",
            "fee": 500
          },
          {
            "oldAddress": "0x00a4dfb447a43a583d8e07eae9d4efbb3656cbcb",
            "newAddress": "0xad4c666fc170b468b19988959eb931a3676f0e9f",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x6fd9d7AD17242c41f7131d257212c54A0e816691",
            "fee": 3000
          },
          {
            "oldAddress": "0xdeb1106b510d94df3bcc55e74f51a6f6b231d97e",
            "newAddress": "0x4983691a26d55eb9e18d2e12e3b770cdd3f76a5f",
            "token0": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 3000
          },
          {
            "oldAddress": "0xd2243a43813cd7c4bfb2287f32d3989b0f2f67d5",
            "newAddress": "0x8e2eaef2c05ef93f424a8324b94e725eaa362f91",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x6fd9d7AD17242c41f7131d257212c54A0e816691",
            "fee": 10000
          },
          {
            "oldAddress": "0x0b3a6896345b68539571aab140134630151ebc68",
            "newAddress": "0x8531e48a8611729185be9eaad945acbd6b32e256",
            "token0": "0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6",
            "token1": "0x4200000000000000000000000000000000000006",
            "fee": 500
          },
          {
            "oldAddress": "0x99959743247f2fa2e97b33e532337eae616beeda",
            "newAddress": "0xeb1817b708415f4f78c5f0c99cbbd6a3a899fa6d",
            "token0": "0x6fd9d7AD17242c41f7131d257212c54A0e816691",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 3000
          },
          {
            "oldAddress": "0x94ee41af02171d6dc1e05b790de547fa50dbd7cf",
            "newAddress": "0x26e7fed14a97e0c482a302237971cf1b04f6d3e9",
            "token0": "0x6fd9d7AD17242c41f7131d257212c54A0e816691",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 3000
          },
          {
            "oldAddress": "0x7613311fbb6a4580cdd602f9978c317b2a783d5f",
            "newAddress": "0x3926a81afe5c9c3d05296e4fac4728ba5411ac78",
            "token0": "0x6fd9d7AD17242c41f7131d257212c54A0e816691",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 10000
          },
          {
            "oldAddress": "0x89fe55759966831d747669bfbda477ebf09475d6",
            "newAddress": "0x7a5ea63fe3430a3b9a06fd80a4a9afaa17c1e878",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0xB27E3Eab7526bF721ea8029bFcd3fDc94c4f8b5b",
            "fee": 10000
          },
          {
            "oldAddress": "0x1c536614fd8ed5faba94528782fbc886c426651a",
            "newAddress": "0xf5a389030a565c13d6e6bbe9342ac9d31dc7521a",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0xE405de8F52ba7559f9df3C368500B6E6ae6Cee49",
            "fee": 3000
          },
          {
            "oldAddress": "0xb0e9a44258cce8ef36c87e8f252aa6bf7cd4b245",
            "newAddress": "0x6168ec836d0b1f0c37381ec7ed1891a412872121",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "fee": 3000
          },
          {
            "oldAddress": "0xcd7b42cee81a3394ee58dab93bbfc87cab03adb5",
            "newAddress": "0x2024c394741a5301e89a375b7bf52f865bc166fd",
            "token0": "0x6fd9d7AD17242c41f7131d257212c54A0e816691",
            "token1": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "fee": 3000
          },
          {
            "oldAddress": "0x88f8cd42570f74ff3ef5acd090419070c6efe37a",
            "newAddress": "0x91cca461ee9435848ac0da8fc416ad0816272786",
            "token0": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 3000
          },
          {
            "oldAddress": "0xd4bbfe5b58381ba4b9ce87146ce9e5a2d1057d3e",
            "newAddress": "0x865d39d66dee5719e6bee98885ef40b9a36bf56e",
            "token0": "0x6fd9d7AD17242c41f7131d257212c54A0e816691",
            "token1": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "fee": 500
          },
          {
            "oldAddress": "0x9b8ad5085af53eff13f3ddadcafa453549f7a93f",
            "newAddress": "0x1fff624960ff9d0556420f3647d6aaf06389aab1",
            "token0": "0x6fd9d7AD17242c41f7131d257212c54A0e816691",
            "token1": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "fee": 3000
          },
          {
            "oldAddress": "0x188530f0c09e56e6e30dd5ef76a9b3f0dc403763",
            "newAddress": "0xc8c07386e29f3f239b91019d5426ae139c5bd17b",
            "token0": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "token1": "0x96db852D93c2feA0F447D6Ec22E146e4e09Caee6",
            "fee": 10000
          },
          {
            "oldAddress": "0x704baee64df71741cf3029652dc99101adc846f0",
            "newAddress": "0x1b19825a9e32b1039080acb1e1f9271314938b96",
            "token0": "0x7FB688CCf682d58f86D7e38e03f9D22e7705448B",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 500
          },
          {
            "oldAddress": "0xe4decbe898a6b8bb79ac48e93681cd04d7b1ca1b",
            "newAddress": "0x602a4d0f9e8d40ad3f620050efd1690da908dc0d",
            "token0": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "token1": "0x7FB688CCf682d58f86D7e38e03f9D22e7705448B",
            "fee": 500
          },
          {
            "oldAddress": "0x251144c131413a5f6e54001cb586f9101b447059",
            "newAddress": "0x345ddb5743859efce0e6e8293ebd35373d34b6c7",
            "token0": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "token1": "0xE405de8F52ba7559f9df3C368500B6E6ae6Cee49",
            "fee": 10000
          },
          {
            "oldAddress": "0x4065c249115481baaec5c6a16929592935d29ec1",
            "newAddress": "0x94ad9a19126ebb02dda874237e5820fd4943f5de",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "fee": 10000
          },
          {
            "oldAddress": "0x6c66eb2798bf42455b63cff3fa3e5bcc3d31848f",
            "newAddress": "0x905707e5c7a10e8351bbd03347be8b5f5de7301a",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0x6fd9d7AD17242c41f7131d257212c54A0e816691",
            "fee": 10000
          },
          {
            "oldAddress": "0xa0eed53ea02a174e4ee81d88d3970b5198580b52",
            "newAddress": "0x7d1602f342787f80aef458c10e741149a1697447",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0x6fd9d7AD17242c41f7131d257212c54A0e816691",
            "fee": 3000
          },
          {
            "oldAddress": "0xb61a5a79a83ff386dbe40a1bc95578856ab2fa5f",
            "newAddress": "0xa7bb0d95c6ba0ed0aca70c503b34bc7108589a47",
            "token0": "0x68f180fcCe6836688e9084f035309E29Bf0A2095",
            "token1": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "fee": 500
          },
          {
            "oldAddress": "0xc102e1de27d8467589cc65f4b4b18d534f6fdac6",
            "newAddress": "0xb0eca217602b031e03956553fb510085c9f2df28",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x8F69Ee043d52161Fd29137AeDf63f5e70cd504D5",
            "fee": 3000
          },
          {
            "oldAddress": "0x61057f7f7c2e338c36fd29433d7977b618348cd0",
            "newAddress": "0x320616dbe138aa2f3db7a5a46ba79a13032cc5f2",
            "token0": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "token1": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "fee": 3000
          },
          {
            "oldAddress": "0x05c1d7c9b9b4f1c38859681bd7b4eebb4c373a8e",
            "newAddress": "0xba213008fe93b3591e439f3b2aa51b3e4a2bd7c7",
            "token0": "0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6",
            "token1": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "fee": 3000
          },
          {
            "oldAddress": "0xbf592a3a4c64c8c28b667d060336e25480fe6c48",
            "newAddress": "0x680b4eb8b9b8533d503a545adad4af9f00df5f05",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x7C17611Ed67D562D1F00ce82eebD39Cb7B595472",
            "fee": 10000
          },
          {
            "oldAddress": "0xb2ab739b499ff9fa019ff944135b4974942b3a95",
            "newAddress": "0x296b88b607ea3a03c821ca4dc34dd9e7e4efa041",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x7FB688CCf682d58f86D7e38e03f9D22e7705448B",
            "fee": 10000
          },
          {
            "oldAddress": "0x3e9ef76529932226742113984e6a6c7cea7e2452",
            "newAddress": "0xa99638e4ac81d4ce32c945c1415f89ab8d86bf2c",
            "token0": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "token1": "0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9",
            "fee": 10000
          },
          {
            "oldAddress": "0xcb590932a77e02aac00c83bbba4d8014efbebb89",
            "newAddress": "0x9bb3267c4c3e69a961479c475f8fcc4c300af5bd",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x7FB688CCf682d58f86D7e38e03f9D22e7705448B",
            "fee": 3000
          },
          {
            "oldAddress": "0x3643c5840fc0ccf4f667a35a151e10302d4d0d23",
            "newAddress": "0x65f8a80d8049a77619435f841055fa4c8d785c47",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0x96db852D93c2feA0F447D6Ec22E146e4e09Caee6",
            "fee": 500
          },
          {
            "oldAddress": "0x805b9cf595282d807adfe84a89bec85be5d07f53",
            "newAddress": "0xd3265ea86af798659b4132a453e7cdb29b877e10",
            "token0": "0x96db852D93c2feA0F447D6Ec22E146e4e09Caee6",
            "token1": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "fee": 500
          },
          {
            "oldAddress": "0x1a718270a5b014209fb77ac2985556ee471b29af",
            "newAddress": "0x8dfc59e8b119bffa5f552642028e005b1972edc4",
            "token0": "0x6fd9d7AD17242c41f7131d257212c54A0e816691",
            "token1": "0x96db852D93c2feA0F447D6Ec22E146e4e09Caee6",
            "fee": 500
          },
          {
            "oldAddress": "0xb91cf01b64c6e6540c45ae356554599cbe92831f",
            "newAddress": "0xc210aeb4e84e0c3b6ee5816858984d52d04f0219",
            "token0": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "token1": "0x96db852D93c2feA0F447D6Ec22E146e4e09Caee6",
            "fee": 500
          },
          {
            "oldAddress": "0x8956827b23063c82d0c697004f0015b454a2f107",
            "newAddress": "0x9aaa481a863e95168c01f23640b357b014dff09a",
            "token0": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
            "token1": "0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4",
            "fee": 500
          },
          {
            "oldAddress": "0x86cf7e458ce79afe44924263d58ef1fd57d1b57c",
            "newAddress": "0x25cc77a38f8de3b9b090fea8f0f5995c4e10a386",
            "token0": "0x4200000000000000000000000000000000000006",
            "token1": "0xe0BB0D3DE8c10976511e5030cA403dBf4c25165B",
            "fee": 10000
          }
        ]'
      ) data(json_column) 
) 

SELECT
    from_hex( json_extract_scalar(json_data, '$[' || cast(t.index as varchar) || '].oldAddress') ) AS oldAddress,
    from_hex( json_extract_scalar(json_data, '$[' || cast(t.index as varchar) || '].newAddress') ) AS newAddress,
    from_hex( json_extract_scalar(json_data, '$[' || cast(t.index as varchar) || '].token0') ) AS token0,
    from_hex( json_extract_scalar(json_data, '$[' || cast(t.index as varchar) || '].token1') ) AS token1,
    cast( json_extract_scalar(json_data, '$[' || cast(t.index as varchar) || '].fee') as bigint) AS fee,
    cast('2021-01-14' as timestamp) as creation_block_time,
    0 as creation_block_number,
    0x0000000000000000000000000000000000000000 as contract_address
FROM ovm1_legacy_pools_raw
CROSS JOIN UNNEST(sequence(0, json_array_length(json_data) - 1)) AS t(index)