SELECT block.slot_no
     , tx.block_index
     ,        datum.hash,        datum.bytes
     , inline_datum.hash, inline_datum.bytes
     , tx_out.index
  FROM block
  JOIN tx ON tx.block_id = block.id
  LEFT JOIN tx_out ON tx_out.tx_id = block.id
  LEFT JOIN datum inline_datum ON tx.inline_datum_id = inline_datum.id
  LEFT JOIN datum ON tx.data_hash_id = datum.hash
