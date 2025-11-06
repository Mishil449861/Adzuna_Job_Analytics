MERGE INTO `ba882-team4-474802.ba882_jobs.locations` AS T
USING (
  -- Select distinct, non-null locations
  SELECT DISTINCT city, state, country
  FROM `ba882-team4-474802.ba882_jobs.staging_locations`
  WHERE city IS NOT NULL OR state IS NOT NULL OR country IS NOT NULL
) AS S
ON IFNULL(T.city, '') = IFNULL(S.city, '') 
   AND IFNULL(T.state, '') = IFNULL(S.state, '')
   AND IFNULL(T.country, '') = IFNULL(S.country, '')
WHEN NOT MATCHED BY TARGET THEN
  INSERT (city, state, country)
  VALUES (S.city, S.state, S.country);