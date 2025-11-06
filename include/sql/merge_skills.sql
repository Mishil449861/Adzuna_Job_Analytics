MERGE INTO `ba882-team4-474802.ba882_jobs.skills` AS T
USING `ba882-team4-474802.ba882_jobs.staging_skills` AS S
ON T.skill_id = S.skill_id
WHEN MATCHED THEN
  UPDATE SET
    T.skill_name = S.skill_name,
    T.skill_type = S.skill_type,
    T.category = S.category,
    T.subcategory = S.subcategory,
    T.description = S.description,
    T.tags = S.tags,
    T.is_soft_skill = S.is_soft_skill,
    T.info_url = S.info_url,
    T.ingest_ts = S.ingest_ts
WHEN NOT MATCHED BY TARGET THEN
  INSERT (skill_id, skill_name, skill_type, category, subcategory, description, tags, is_soft_skill, info_url, ingest_ts)
  VALUES (skill_id, skill_name, skill_type, category, subcategory, description, tags, is_soft_skill, info_url, ingest_ts);
