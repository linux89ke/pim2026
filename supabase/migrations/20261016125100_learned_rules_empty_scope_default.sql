-- Do not silently assign Kenya when a writer fails to provide a country.
ALTER TABLE public.learned_image_rules
    ALTER COLUMN countries SET DEFAULT ARRAY[]::text[];
