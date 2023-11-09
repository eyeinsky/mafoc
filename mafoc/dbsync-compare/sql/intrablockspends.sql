SELECT block.slot_no
     , sum(block.id = block1.id)
     , count(*)
  FROM block
  JOIN tx ON tx.block_id = block.id
  JOIN tx_in ON tx_in.tx_out_id = tx.id
  JOIN tx tx1 ON tx1.id = tx_in.tx_in_id
  JOIN block block1 ON tx1.block_id = block1.id
 GROUP BY block.id
