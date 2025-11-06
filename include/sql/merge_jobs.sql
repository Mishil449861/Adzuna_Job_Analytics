MERGE INTO `ba882-team4-474802.ba882_jobs.jobs` AS T
USING `ba882-team4-474802.ba882_jobs.staging_jobs` AS S
ON T.job_id = S.job_id
WHEN MATCHED THEN
  UPDATE SET
    T.title = S.title,
    T.description = S.description,
    T.salary_min = S.salary_min,
    T.salary_max = S.salary_max,
    T.created = IF(
        S.created IS NULL OR S.created <= 0 OR S.created > 253402300799999999, 
        NULL, 
        TIMESTAMP_TRUNC(TIMESTAMP_MICROS(S.created), MINUTE)
    ),
    T.redirected_url = S.redirected_url,
    T.ingest_data = S.ingest_data,
    T.ingest_ts = IF(
        S.ingest_ts IS NULL OR S.ingest_ts <= 0 OR S.ingest_ts > 253402300799999999, 
        NULL, 
        TIMESTAMP_TRUNC(TIMESTAMP_MICROS(S.ingest_ts), MINUTE)
    ),
    T.company_name = S.company_name,
    T.category_label = S.category_label,
    T.city = S.city,
    T.state = S.state
WHEN NOT MATCHED BY TARGET THEN
  INSERT (job_id, title, description, salary_min, salary_max, created, redirected_url, ingest_data, ingest_ts, company_name, category_label, city, state)
  VALUES (
    job_id, 
    title, 
    description, 
    salary_min, 
    salary_max, 
    IF(
        created IS NULL OR created <= 0 OR created > 253402300799999999, 
        NULL, 
        TIMESTAMP_TRUNC(TIMESTAMP_MICROS(created), MINUTE)
    ),
    redirected_url, 
    ingest_data, 
    IF(
        ingest_ts IS NULL OR ingest_ts <= 0 OR ingest_ts > 253402300799999999, 
        NULL, 
        TIMESTAMP_TRUNC(TIMESTAMP_MICROS(ingest_ts), MINUTE)
    ),
    company_name, 
    category_label, 
    city, 
    state
  );