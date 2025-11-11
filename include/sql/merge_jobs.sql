MERGE INTO `ba882-team4-474802.ba882_jobs.jobs` AS target
USING (
    -- This outer query selects only 1 row per job_id,
    -- preferring the one with the latest ingest_ts
    SELECT *
    FROM (
        -- This is your original subquery, now with ROW_NUMBER()
        SELECT
            s.job_id,
            s.title,
            s.description,
            s.salary_min,
            s.salary_max,
            
            -- FIX 1: Cast timestamps (assuming nanoseconds in staging)
            -- IF(condition, null, conversion) handles bad data
            IF(s.created IS NULL OR s.created <= 0, NULL, TIMESTAMP_MICROS(CAST(s.created / 1000 AS INT64))) AS created, 
            s.redirected_url,
            s.ingest_data,
            IF(s.ingest_ts IS NULL OR s.ingest_ts <= 0, NULL, TIMESTAMP_MICROS(CAST(s.ingest_ts / 1000 AS INT64))) AS ingest_ts,
            
            -- FIX 2: Get the Foreign Keys
            c.company_id,
            cat.category_id,
            loc.location_id,
            
            -- Keep old strings for now
            s.company_name,
            s.category_label,
            s.city,
            s.state,
            
            -- FIX 3: Deduplication logic
            ROW_NUMBER() OVER(PARTITION BY s.job_id ORDER BY s.ingest_ts DESC) as rn
            
        FROM `ba882-team4-474802.ba882_jobs.staging_jobs` AS s
        
        LEFT JOIN `ba882-team4-474802.ba882_jobs.companies` AS c
            ON s.company_name = c.company_name
        LEFT JOIN `ba882-team4-474802.ba882_jobs.categories` AS cat
            ON s.category_label = cat.category_label
        LEFT JOIN `ba882-team4-474802.ba882_jobs.locations` AS loc
            ON IFNULL(s.city, '') = IFNULL(loc.city, '') 
               AND IFNULL(s.state, '') = IFNULL(loc.state, '')
               -- Add country if it's also in your staging_jobs table
               -- AND IFNULL(s.country, '') = IFNULL(loc.country, '')

    )
    WHERE rn = 1  -- This is the FIX: only select the #1 row for each job
    
) AS source
ON target.job_id = source.job_id

-- If job exists, update it with new info AND the new foreign keys
WHEN MATCHED THEN
    UPDATE SET
        target.title = source.title,
        target.description = source.description,
        target.salary_min = source.salary_min,
        target.salary_max = source.salary_max,
        target.created = source.created,
        target.ingest_ts = source.ingest_ts,
        target.company_id = source.company_id,     -- Update the ID
        target.category_id = source.category_id, -- Update the ID
        target.location_id = source.location_id,   -- Update the ID
        target.company_name = source.company_name, -- Update the old string
        target.category_label = source.category_label,
        target.city = source.city,
        target.state = source.state

-- If it's a new job, insert it with all columns, including the new foreign keys
WHEN NOT MATCHED THEN
    INSERT (
        job_id, title, description, salary_min, salary_max, created, redirected_url, ingest_data, ingest_ts,
        company_id, category_id, location_id,
        company_name, category_label, city, state
    )
    VALUES (
        source.job_id, source.title, source.description, source.salary_min, source.salary_max, source.created,
        source.redirected_url, source.ingest_data, source.ingest_ts,
        source.company_id, source.category_id, source.location_id,
        source.company_name, source.category_label, source.city, source.state
    );