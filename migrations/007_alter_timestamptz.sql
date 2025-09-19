-- Normalize to timestamptz (UTC) to avoid timezone ambiguity
-- token_transfer_hourly.hour may have been created as timestamp; convert to timestamptz
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name='token_transfer_hourly' AND column_name='hour' AND data_type='timestamp without time zone'
    ) THEN
        ALTER TABLE token_transfer_hourly 
        ALTER COLUMN hour TYPE timestamptz USING (hour AT TIME ZONE 'UTC');
    END IF;
END $$;

-- token_8h_cache.base_hour may have been created as timestamp; convert to timestamptz
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name='token_8h_cache' AND column_name='base_hour' AND data_type='timestamp without time zone'
    ) THEN
        ALTER TABLE token_8h_cache 
        ALTER COLUMN base_hour TYPE timestamptz USING (base_hour AT TIME ZONE 'UTC');
    END IF;
END $$;

