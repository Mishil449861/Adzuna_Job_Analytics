MERGE INTO `ba882-team4-474802.ba882_jobs.categories` AS T
USING (
  -- Select distinct, non-null categories
  SELECT DISTINCT category_label
  FROM `ba882-team4-474802.ba882_jobs.staging_categories`
  WHERE category_label IS NOT NULL
) AS S
ON T.category_label = S.category_label
WHEN NOT MATCHED BY TARGET THEN
  INSERT (category_label)
  VALUES (S.category_label);