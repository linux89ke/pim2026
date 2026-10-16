-- Keep every existing learned image rule scoped to Kenya until other markets
-- are explicitly enabled and reviewed.
UPDATE public.learned_image_rules
SET countries = ARRAY['KE']::text[]
WHERE countries IS DISTINCT FROM ARRAY['KE']::text[];

ALTER TABLE public.learned_image_rules
    ALTER COLUMN countries SET DEFAULT ARRAY['KE']::text[];
