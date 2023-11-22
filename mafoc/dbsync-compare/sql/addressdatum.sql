SELECT tx_out.address
     , tx_out.data_hash
     , block.slot_no
  FROM block
  JOIN tx ON tx.block_id = block.id
  JOIN tx_out ON tx_out.tx_id = tx.id
