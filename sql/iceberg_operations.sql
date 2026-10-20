-- Read-only Iceberg metadata examples for Trino.
-- Run with: make iceberg-history

-- Review recent table snapshots and write operations.
SELECT
  committed_at,
  snapshot_id,
  parent_id,
  operation
FROM gold."daily_revenue$snapshots"
ORDER BY committed_at DESC;

-- Review when snapshots became the current table state.
SELECT
  made_current_at,
  snapshot_id,
  parent_id,
  is_current_ancestor
FROM gold."daily_revenue$history"
ORDER BY made_current_at DESC;

-- Inspect files that make up the current snapshot.
SELECT
  file_path,
  record_count,
  file_size_in_bytes,
  file_format
FROM gold."daily_revenue$files"
ORDER BY file_path;

-- Replace 1234567890123456789 with a snapshot_id from the first query to read
-- the table as it existed at that snapshot.
-- SELECT *
-- FROM gold.daily_revenue FOR VERSION AS OF 1234567890123456789
-- ORDER BY order_date, region;