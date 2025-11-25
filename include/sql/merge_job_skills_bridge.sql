MERGE INTO `ba882-team4-474802.ba882_jobs.job_skills_bridge` AS target
USING (
    -- Join the raw extraction table with the clean skills dimension to get IDs
    SELECT DISTINCT
        raw.source_job_id as job_id,
        dim.skill_id
    FROM `ba882-team4-474802.ba882_jobs.job_skills` raw
    JOIN `ba882-team4-474802.ba882_jobs.skills` dim
        ON raw.skill_name = dim.skill_name
    WHERE raw.source_job_id IS NOT NULL
) AS source
ON target.job_id = source.job_id AND target.skill_id = source.skill_id

-- If this specific job-skill link doesn't exist, add it
WHEN NOT MATCHED THEN
  INSERT (job_id, skill_id)
  VALUES (source.job_id, source.skill_id);