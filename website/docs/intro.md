---
sidebar_position: 0
---

# Introduction

Mafoc is next generation chain indexer for Cardano.

Achitecture boils down to fold over blocks in IO:

```haskell
type Indexer = (Block -> State -> IO (State, Event)) -> IO State -> IO Block -> IO State
--             ^ step
--                                                      ^ init
--                                                                  ^ get next
```

<div style={{display:'none'}}>
type Step = Block -> State -> IO (State, Event)
type Init = IO State
type GetNextBlock = IO Block
type Indexer = Step -> Init -> IO Block -> IO State
</div>

IO, well because we need to get blocks from the outside world and also
we need to store the processed data to the outside world.

For any indexer, the operation of this is implemented in the `Indexer`
type class where state, init and step have indexer-specific
implementations.

## Library

The purpose of Mafoc is to be used as a library to implement new
indexers. The two things which need implemented are:
1. what data to pick from the block
2. how to store it

Rest is provided by the library. Corollary: if library improves, so
does the user's indexer.

## Predefined indexers

There are [plenty of predefined](./category/indexers) indexers which can be run
out-of-the-box:

```bash
docker run --rm -it markus77/mafoc <indexer> <indexer options>
```
