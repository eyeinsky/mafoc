SELECT MIN(block.slot_no)
     , tx_out.address_raw
  FROM block
  JOIN tx     ON block.id = tx.block_id
  JOIN tx_out ON tx_out.tx_id = tx.id
