MERGE INTO `ba882-team4-474802.ba882_jobs.jobstats` AS T
USING `ba882-team4-474802.ba882_jobs.staging_jobstats` AS S
ON T.job_id = S.job_id
WHEN MATCHED THEN
  UPDATE SET
    T.contract_type = S.contract_type,
    T.contract_time = S.contract_time,
    T.posting_week = S.posting_week
WHEN NOT MATCHED BY TARGET THEN
  INSERT (job_id, contract_type, contract_time, posting_week)
  VALUES (job_id, contract_type, contract_time, posting_week);