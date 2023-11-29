---
sidebar_position: 4
---

# Development environment

Mafoc depends is written in Haskell, uses Nix and Cabal, and depends on
[cardano-api](https://github.com/input-output-hk/cardano-api) library
for acquiring a local chainsync connection and extracting specific
data from blocks.

Enter development environment with
```
git clone https://github.com/eyeinsky/mafoc
cd mafoc
nix develop
```

All regular Cabal commands such as (`build`, `test`, `clean` etc) now work.
