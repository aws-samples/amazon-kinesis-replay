WITH 
  check_rows AS (
    SELECT trip_id, 1 + trip_id - row_number() OVER (ORDER BY trip_id) AS diff
    FROM "producer-test"."shausma_producer"
    WHERE partition_0 = 'test-4' AND type = 'trip'
  )
SELECT max(diff), min(diff) FROM check_rows;
