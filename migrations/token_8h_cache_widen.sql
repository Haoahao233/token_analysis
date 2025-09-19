-- Widen counts to integer[] and sum8 to bigint to avoid overflow
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name='token_8h_cache' AND column_name='counts' AND udt_name='_int2'
    ) THEN
        ALTER TABLE token_8h_cache 
        ALTER COLUMN counts TYPE integer[] USING (ARRAY(SELECT x::integer FROM unnest(counts) AS x));
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name='token_8h_cache' AND column_name='sum8' AND data_type='integer'
    ) THEN
        ALTER TABLE token_8h_cache 
        ALTER COLUMN sum8 TYPE bigint;
    END IF;
END $$;

