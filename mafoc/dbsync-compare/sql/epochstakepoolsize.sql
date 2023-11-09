SELECT ph.hash_raw AS pool_hash
     , sum(amount) AS sum_amount
  FROM epoch_stake es
  JOIN pool_hash ph ON es.pool_id = ph.id
 GROUP BY epoch_no, pool_hash
 ORDER BY sum_amount desc
