SELECT block.slot_no
     , block.hash
     , tx_out.payment_cred
     -- , tx.cbor -- No transaction CBOR in Cardano DB Sync (?).
  FROM block
  JOIN tx ON tx.block_id = block.id
  JOIN tx_out ON tx_out.tx_id = block.id
 WHERE address_has_script
