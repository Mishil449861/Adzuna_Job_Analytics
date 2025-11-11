MERGE INTO `ba882-team4-474802.ba882_jobs.companies` AS target
USING (
    -- Get unique, non-null names from staging
    SELECT DISTINCT company_name
    FROM `ba882-team4-474802.ba882_jobs.staging_companies`
    WHERE company_name IS NOT NULL
) AS source
ON target.company_name = source.company_name
-- When it's a new company, insert its name AND a new primary key
WHEN NOT MATCHED THEN
  INSERT (company_name, company_id)
  VALUES (source.company_name, GENERATE_UUID());