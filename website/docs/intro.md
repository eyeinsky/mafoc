---
sidebar_position: 0
---

# Introduction

If you have access to a cardano-node then you can run any of the
predefined indexers below.

Follow tip for incoming deposits to addresses:
```
mafoc deposit /path/to/mainnet --address addr1.. --address addr2..
```

Get addresses' ADA balance (interval needs to be *before* the very first deposit):
```
mafoc addressbalance /path/to/mainnet --address addr1.. --address
addr2.. -- interval todo
```

Monitor custom asset creation and burn:
```
mafoc mintburn /path/to/mainnet TODO
```

Create header database of slot_no and block_header_hash
tuples. (This can then be used as --header-db argument for slot-only
based interval specifications.)
```
mafoc headerdb /path/to/mainnet TODO
```


Mafoc has the following indexers predefined:

```
  addressbalance           Index balance changes for a set of addresses
  addressdatum             Index datums for addresses
  blockbasics              Index slot number, block header hash and number of
                           transactions per block
  deposit                  Index deposits for a set of addresses
  datum                    Index datums by hash
  epochnonce               Index epoch numbers and epoch nonces
  epochstakepoolsize       Index stakepool sizes (in ADA) per epoch
  mamba                    All mamba indexers
  mintburn                 Index minting and burning of custom assets
  noop                     Don't index anything, but drain blocks over local
                           chainsync to measure speed ceiling
  scripttx                 Index transactions with scripts
  utxo                     Index transaction outputs
  intrablockspends         Report the number of intra-block spends: where
                           transaction spends UTxOs from the same block it
                           itself is in.
```
