-- Fake/counterfeit image decisions are platform-wide, regardless of upload
-- market. Keep other learned reasons scoped to their learned markets.
UPDATE public.learned_image_rules
SET countries = ARRAY['*']::text[]
WHERE lower(trim(flag)) IN (
    'suspected fake product',
    'counterfeit sneakers',
    'suspected counterfeit jerseys'
)
OR lower(trim(flag)) LIKE '%replica jersey%ip violation%';
