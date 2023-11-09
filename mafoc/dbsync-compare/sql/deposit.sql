SELECT block.slot_no
     , block.hash
     , tx.hash
     , tx_out.address
     , tx_out.value
  FROM block
  JOIN tx ON tx.block_id = block.id
  JOIN tx_out ON tx_out.tx_id = block.id
