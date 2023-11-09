SELECT block.slot_no
     , block.hash
     , tx.hash
     , SUM(tx_out.value) OVER (ORDER BY .. ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                         AS balance
     , value
  FROM blocks, tx, tx_out
  JOIN tx ON tx.block_id = block.id
  JOIN tx_out ON tx_out.tx_id = block.id
