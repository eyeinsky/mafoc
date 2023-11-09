SELECT block.slot_no
     , block.hash
     , count(*)
  FROM block
  JOIN tx ON tx.block_id = block.id
