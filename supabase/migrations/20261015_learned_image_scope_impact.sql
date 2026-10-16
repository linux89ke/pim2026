-- Learned image rule market scopes and lightweight impact metadata.
-- Safe to run repeatedly on the existing Supabase table.
ALTER TABLE public.learned_image_rules
    ADD COLUMN IF NOT EXISTS countries text[] NOT NULL DEFAULT ARRAY['*']::text[],
    ADD COLUMN IF NOT EXISTS match_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS matched_sids jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS matched_sellers jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS impact_truncated boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS brand_infringed_by_country jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS scope_categories jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS scope_brands jsonb NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS learned_image_rules_countries_gin
    ON public.learned_image_rules USING gin (countries);
