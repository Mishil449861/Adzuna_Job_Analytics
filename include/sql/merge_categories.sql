MERGE INTO `ba882-team4-474802.ba882_jobs.categories` AS target
USING (
    -- Get unique, non-null labels from staging
    SELECT DISTINCT category_label
    FROM `ba882-team4-474802.ba882_jobs.staging_categories`
    WHERE category_label IS NOT NULL
) AS source
ON target.category_label = source.category_label
-- When it's a new category, insert its label AND a new primary key
WHEN NOT MATCHED THEN
  INSERT (category_label, category_id)
  VALUES (source.category_label, GENERATE_UUID());