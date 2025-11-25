CREATE OR REPLACE TABLE `ba882-team4-474802.ba882_jobs.category_industry_overlap` AS
SELECT
    category_label,
    -- Create a nested list of the top 5 companies for this category
    ARRAY_AGG(
        STRUCT(company_name, count)
        ORDER BY count DESC LIMIT 5
    ) as top_companies,
    -- Calculate the total jobs for this category
    SUM(count) as total_jobs
FROM (
    -- Inner query: Count jobs per unique Category-Company pair
    SELECT
        j.category_label,
        c.company_name,
        COUNT(DISTINCT j.job_id) as count
    FROM `ba882-team4-474802.ba882_jobs.jobs` j
    JOIN `ba882-team4-474802.ba882_jobs.companies` c
        ON j.company_id = c.company_id
    WHERE j.category_label IS NOT NULL AND c.company_name IS NOT NULL
    GROUP BY j.category_label, c.company_name
)
GROUP BY category_label
ORDER BY total_jobs DESC;