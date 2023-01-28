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

type Map = Block -> Data
type Fold = Block -> State -> Data

Examples of a mapping indexer:
- at which transactions was my smart contract (plutus script) executed
- how many transaction

Examples of a folding indexer:
- how much ADA does a stakepool have staked: to know the absolute
  amount one would need to know how much was staked previously, and
  how much did it change with the current transaction;
- address balance: how much ADA did an address have before and how
  much did it change with the current transaction;

Current:
- block transaction count: count transactions for every block
  - database block_tx_count()
