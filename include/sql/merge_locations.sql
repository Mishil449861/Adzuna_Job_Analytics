MERGE INTO `ba882-team4-474802.ba882_jobs.locations` AS target
USING (
    -- Get unique, non-null location combinations from staging
    SELECT DISTINCT city, state, country
    FROM `ba882-team4-474802.ba882_jobs.staging_locations`
    WHERE city IS NOT NULL OR state IS NOT NULL OR country IS NOT NULL
) AS source
-- Use IFNULL to safely match on potentially null fields
ON IFNULL(target.city, '') = IFNULL(source.city, '') 
   AND IFNULL(target.state, '') = IFNULL(source.state, '')
   AND IFNULL(target.country, '') = IFNULL(source.country, '')
-- When it's a new location, insert it AND a new primary key
WHEN NOT MATCHED THEN
  INSERT (city, state, country, location_id)
  VALUES (source.city, source.state, source.country, GENERATE_UUID());