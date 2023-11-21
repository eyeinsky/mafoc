SELECT block.slot_no
     , block.hash
     , multi_asset.policy
     , multi_asset.name
     , multi_asset.fingerprint
  FROM block
  JOIN tx          ON block.id = tx.block_id
  JOIN ma_tx_mint  ON tx.id = ma_tx_mint.tx_id
  JOIN multi_asset ON ma_tx_mint.ident = multi_asset.id
