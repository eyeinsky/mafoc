SELECT block.slot_no
     , block.hash
     , block.block_no
     , tx.block_index
     , tx.hash
     , tx_out.index
     , tx_out.value
     , tx_out.address_raw
     , tx_out.data_hash
  FROM block
  JOIN tx ON tx.block_id = block.id
  JOIN tx_out ON tx_out.tx_id = tx.id
  LEFT JOIN tx_in ON tx_in.tx_out_id = tx.id AND tx_in.tx_out_index = tx_out.index
  LEFT JOIN tx spending_tx ON spending_tx.id = tx_in.tx_in_id
  LEFT JOIN block spending_block ON spending_block.id = spending_tx.block_id
