MERGE INTO `ba882-team4-474802.ba882_jobs.companies` AS T
USING (
  -- Select distinct, non-null companies from the staging table
  SELECT DISTINCT company_name
  FROM `ba882-team4-474802.ba882_jobs.staging_companies`
  WHERE company_name IS NOT NULL
) AS S
ON T.company_name = S.company_name
WHEN NOT MATCHED BY TARGET THEN
  INSERT (company_name)
  VALUES (S.company_name);