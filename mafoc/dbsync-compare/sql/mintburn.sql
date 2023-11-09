SELECT block.slot_no
     , block.hash
     , block.block_no
     , tx.block_index
     , tx.hash
     , multi_asset.policy
     , multi_asset.name
     , ma_tx_mint.quantity
     , redeemer_join.bytes
     , redeemer_join.hash
  FROM block
  JOIN tx          ON block.id = tx.block_id
  JOIN ma_tx_mint  ON tx.id = ma_tx_mint.tx_id
  JOIN multi_asset ON ma_tx_mint.ident = multi_asset.id
  LEFT JOIN
    ( SELECT DISTINCT redeemer.tx_id, redeemer.script_hash, redeemer_data.hash, redeemer_data.bytes
        FROM redeemer
        JOIN redeemer_data ON redeemer_data.id = redeemer.redeemer_data_id
       WHERE redeemer.purpose = 'mint'
    ) redeemer_join ON redeemer_join.tx_id = tx.id
