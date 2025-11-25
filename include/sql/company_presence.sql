CREATE OR REPLACE TABLE `ba882-team4-474802.ba882_jobs.company_presence` AS
SELECT DISTINCT
    company_id,
    location_id
FROM `ba882-team4-474802.ba882_jobs.jobs`
WHERE company_id IS NOT NULL AND location_id IS NOT NULL;