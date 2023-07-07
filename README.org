# Mafoc

*Maps and folds over Cardano blockchain*

To make information on a blockchain readily accessible, it's required
to create auxiliary data structures, i.e *indexes*, to store just the
data needed. *Indexers* are programs that create these indexes.

As a blockchain is literally a sequence of blocks, which itself is a
sequesce of transactions, it's simplest to model indexers as maps or
folds from transactions to indexer-specific states. Compared with
folds, maps are simply a degenerate form of folds where no state needs
to be carried over. As no input state is needed, map type indexes are
also simpler to create as no input state is needed.

Pseoudocode type signatures:

```haskell
type Map = Block -> Event
type Fold = Block -> State -> (Event, State)
```

Examples of a mapping indexer:
- extract at which transactions was my smart contract (plutus script) executed
- how many transaction are in a block

Examples of a folding indexer:

- address balance

  It's a fold, because to know the absolute amount of ADA for any
  given addres, one would need to know both the received-or-spent
  amount, and how much did it have previously (= _state_)

Difference being, with a fold there is a state to be carried.
