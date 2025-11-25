MERGE INTO `ba882-team4-474802.ba882_jobs.skills` AS target
USING (
    -- Get all unique skills from the raw job_skills table
    -- We only want skills that aren't null
    SELECT DISTINCT skill_name
    FROM `ba882-team4-474802.ba882_jobs.job_skills`
    WHERE skill_name IS NOT NULL
) AS source
ON target.skill_name = source.skill_name

-- If the skill is new (not in the dimension table yet), insert it with a new UUID
WHEN NOT MATCHED THEN
  INSERT (skill_id, skill_name)
  VALUES (GENERATE_UUID(), source.skill_name);