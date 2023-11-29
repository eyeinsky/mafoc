---
sidebar_position: 5
---

# Features

- stop and resume indexing anywhere
- data-compliant with Cardano DB Sync
- "sloppy node": specify node data folder and mafoc will find both the
  socket and configration file from within, no need to specify them
  separately
- Smart start: indexers are smart about at which
  chainpoint they start indexing from. I.e scripttx, datum,
  addressdatum and mintburn indexers start from Alonzo era as prior
  eras don't have data related to them
