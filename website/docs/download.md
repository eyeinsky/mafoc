---
sidebar_position: 50
---

# Download

## Docker (linux, macOS, Windows)

The easiest way to start mafoc is to use docker:
```bash
docker run --rm -it markus77/mafoc
```
The above lists all available indexers.


Start a specific indexer, say, to index datums:
```bash
docker run --rm -it \
    -v /path/to/node.socket:/socket \
    -v /path/to/node/config/folder:/config \
    -v /var/mafoc:/db \
    markus77/mafoc \
    utxo --mainnet -s /socket --node-config /config/config.json --db /db/utxo.db
```
Notice how we mount in cardano-node's socket, configurations' folder,
and also a folder for where to store the database.

## Nix

On NixOS or Linux with the [nix](https://nixos.org/) package manager the following should work:

```bash
nix develop
cabal install mafoc:exe:mafoc
mafoc utxo --mainnet -s /path/to/socket --node-config /path/to/configs --db /var/mafoc/utxo.db
```
