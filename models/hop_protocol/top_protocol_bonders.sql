



FROM (

-- source: https://github.com/hop-protocol/hop/blob/develop/packages/core/src/addresses/mainnet.ts
    USDC: {
      ethereum: {
        optimism: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        arbitrum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        gnosis: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        polygon: '0xa6a688F107851131F0E1dce493EbBebFAf99203e'
      },
      optimism: {
        ethereum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        arbitrum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        gnosis: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        polygon: '0xa6a688F107851131F0E1dce493EbBebFAf99203e'
      },
      arbitrum: {
        ethereum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        optimism: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        gnosis: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        polygon: '0xa6a688F107851131F0E1dce493EbBebFAf99203e'
      },
      gnosis: {
        ethereum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        arbitrum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        optimism: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        polygon: '0xa6a688F107851131F0E1dce493EbBebFAf99203e'
      },
      polygon: {
        ethereum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        arbitrum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        gnosis: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        optimism: '0xa6a688F107851131F0E1dce493EbBebFAf99203e'
      }
    },
    USDT: {
      ethereum: {
        optimism: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        arbitrum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        gnosis: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        polygon: '0xa6a688F107851131F0E1dce493EbBebFAf99203e'
      },
      optimism: {
        ethereum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        arbitrum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        gnosis: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        polygon: '0xa6a688F107851131F0E1dce493EbBebFAf99203e'
      },
      arbitrum: {
        ethereum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        optimism: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        gnosis: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        polygon: '0xa6a688F107851131F0E1dce493EbBebFAf99203e'
      },
      gnosis: {
        ethereum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        arbitrum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        optimism: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        polygon: '0xa6a688F107851131F0E1dce493EbBebFAf99203e'
      },
      polygon: {
        ethereum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        arbitrum: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        gnosis: '0xa6a688F107851131F0E1dce493EbBebFAf99203e',
        optimism: '0xa6a688F107851131F0E1dce493EbBebFAf99203e'
      }
    },
    MATIC: {
      ethereum: {
        gnosis: '0xd8781ca9163e9f132a4d8392332e64115688013a',
        polygon: '0xd8781ca9163e9f132a4d8392332e64115688013a'
      },
      gnosis: {
        ethereum: '0xd8781ca9163e9f132a4d8392332e64115688013a',
        polygon: '0xd8781ca9163e9f132a4d8392332e64115688013a'
      },
      polygon: {
        ethereum: '0xd8781ca9163e9f132a4d8392332e64115688013a',
        gnosis: '0xd8781ca9163e9f132a4d8392332e64115688013a'
      }
    },
    DAI: {
      ethereum: {
        optimism: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        arbitrum: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        gnosis: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        polygon: '0x9298dfD8A0384da62643c2E98f437E820029E75E'
      },
      optimism: {
        ethereum: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        arbitrum: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        gnosis: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        polygon: '0x9298dfD8A0384da62643c2E98f437E820029E75E'
      },
      arbitrum: {
        ethereum: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        optimism: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        gnosis: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        polygon: '0x9298dfD8A0384da62643c2E98f437E820029E75E'
      },
      gnosis: {
        ethereum: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        arbitrum: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        optimism: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        polygon: '0x9298dfD8A0384da62643c2E98f437E820029E75E'
      },
      polygon: {
        ethereum: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        arbitrum: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        gnosis: '0x9298dfD8A0384da62643c2E98f437E820029E75E',
        optimism: '0x9298dfD8A0384da62643c2E98f437E820029E75E'
      }
    },
    ETH: {
      ethereum: {
        optimism: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        arbitrum: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        gnosis: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        polygon: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        nova: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564'
      },
      optimism: {
        ethereum: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        arbitrum: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        gnosis: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        polygon: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        nova: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564'
      },
      arbitrum: {
        ethereum: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        optimism: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        gnosis: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        polygon: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        nova: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564'
      },
      gnosis: {
        ethereum: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        arbitrum: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        optimism: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        polygon: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        nova: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564'
      },
      polygon: {
        ethereum: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        arbitrum: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        gnosis: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        optimism: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        nova: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564'
      },
      nova: {
        ethereum: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        arbitrum: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        gnosis: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        optimism: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564',
        polygon: '0x710bDa329b2a6224E4B44833DE30F38E7f81d564'
      }
    },
    HOP: {
      ethereum: {
        optimism: '0x881296Edcb252080bd476c464cEB521d08df7631',
        arbitrum: '0x881296Edcb252080bd476c464cEB521d08df7631',
        gnosis: '0x881296Edcb252080bd476c464cEB521d08df7631',
        polygon: '0x881296Edcb252080bd476c464cEB521d08df7631'
      },
      optimism: {
        ethereum: '0x881296Edcb252080bd476c464cEB521d08df7631',
        arbitrum: '0x881296Edcb252080bd476c464cEB521d08df7631',
        gnosis: '0x881296Edcb252080bd476c464cEB521d08df7631',
        polygon: '0x881296Edcb252080bd476c464cEB521d08df7631'
      },
      arbitrum: {
        ethereum: '0x881296Edcb252080bd476c464cEB521d08df7631',
        optimism: '0x881296Edcb252080bd476c464cEB521d08df7631',
        gnosis: '0x881296Edcb252080bd476c464cEB521d08df7631',
        polygon: '0x881296Edcb252080bd476c464cEB521d08df7631'
      },
      gnosis: {
        ethereum: '0x881296Edcb252080bd476c464cEB521d08df7631',
        arbitrum: '0x881296Edcb252080bd476c464cEB521d08df7631',
        optimism: '0x881296Edcb252080bd476c464cEB521d08df7631',
        polygon: '0x881296Edcb252080bd476c464cEB521d08df7631'
      },
      polygon: {
        ethereum: '0x881296Edcb252080bd476c464cEB521d08df7631',
        arbitrum: '0x881296Edcb252080bd476c464cEB521d08df7631',
        gnosis: '0x881296Edcb252080bd476c464cEB521d08df7631',
        optimism: '0x881296Edcb252080bd476c464cEB521d08df7631'
      }
    },
    SNX: {
      ethereum: {
        optimism: '0x547d28cDd6A69e3366d6aE3EC39543F09Bd09417'
      },
      optimism: {
        ethereum: '0x547d28cDd6A69e3366d6aE3EC39543F09Bd09417'
      }
    },
    sUSD: {
      ethereum: {
        optimism: '0x547d28cDd6A69e3366d6aE3EC39543F09Bd09417'
      },
      optimism: {
        ethereum: '0x547d28cDd6A69e3366d6aE3EC39543F09Bd09417'
      }
    },
    rETH: {
      ethereum: {
        optimism: '0xD38B96277df34F1f7ac5965F86016E7d02c4Ca94',
        arbitrum: '0xD38B96277df34F1f7ac5965F86016E7d02c4Ca94'
      },
      optimism: {
        ethereum: '0xD38B96277df34F1f7ac5965F86016E7d02c4Ca94',
        arbitrum: '0xD38B96277df34F1f7ac5965F86016E7d02c4Ca94'
      },
      arbitrum: {
        ethereum: '0xD38B96277df34F1f7ac5965F86016E7d02c4Ca94',
        optimism: '0xD38B96277df34F1f7ac5965F86016E7d02c4Ca94'
      }
    },
    MAGIC: {
      ethereum: {
        arbitrum: '0xa251E7519cbCf76e33D4672d2218e3CbcCEB6d60',
        nova: '0xa251E7519cbCf76e33D4672d2218e3CbcCEB6d60'
      },
      arbitrum: {
        ethereum: '0xa251E7519cbCf76e33D4672d2218e3CbcCEB6d60',
        nova: '0xa251E7519cbCf76e33D4672d2218e3CbcCEB6d60'
      },
      nova: {
        ethereum: '0xa251E7519cbCf76e33D4672d2218e3CbcCEB6d60',
        arbitrum: '0xa251E7519cbCf76e33D4672d2218e3CbcCEB6d60'
      }
    }
  }
)