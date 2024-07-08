-- Removal of 'ALTER EXTENSION' where tables were added/removed to/from extension
CREATE OR REPLACE FUNCTION pgmq.drop_queue(queue_name TEXT, partitioned BOOLEAN DEFAULT FALSE)
    RETURNS BOOLEAN AS $$
BEGIN
--     EXECUTE FORMAT(
--             $QUERY$
--         ALTER EXTENSION pgmq DROP TABLE pgmq.q_%s
--         $QUERY$,
--             queue_name
--             );
--
--     EXECUTE FORMAT(
--             $QUERY$
--         ALTER EXTENSION pgmq DROP TABLE pgmq.a_%s
--         $QUERY$,
--             queue_name
--             );

    EXECUTE FORMAT(
            $QUERY$
        DROP TABLE IF EXISTS pgmq.q_%s
        $QUERY$,
            queue_name
            );

    EXECUTE FORMAT(
            $QUERY$
        DROP TABLE IF EXISTS pgmq.a_%s
        $QUERY$,
            queue_name
            );

    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = 'meta' and table_schema = 'pgmq'
    ) THEN
        EXECUTE FORMAT(
                $QUERY$
            DELETE FROM pgmq.meta WHERE queue_name = '%s'
            $QUERY$,
                queue_name
                );
    END IF;

    IF partitioned THEN
        EXECUTE FORMAT(
                $QUERY$
          DELETE FROM public.part_config where parent_table = '%s'
          $QUERY$,
                queue_name
                );
    END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION pgmq.create_non_partitioned(queue_name TEXT)
    RETURNS void AS $$
BEGIN
    PERFORM pgmq.validate_queue_name(queue_name);

    EXECUTE FORMAT(
            $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.q_%s (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB
    )
    $QUERY$,
            queue_name
            );

    EXECUTE FORMAT(
            $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.a_%s (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB
    );
    $QUERY$,
            queue_name
            );

--     IF NOT pgmq._belongs_to_pgmq(FORMAT('q_%s', queue_name)) THEN
--         EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.q_%s', queue_name);
--     END IF;
--
--     IF NOT pgmq._belongs_to_pgmq(FORMAT('a_%s', queue_name)) THEN
--         EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.a_%s', queue_name);
--     END IF;

    EXECUTE FORMAT(
            $QUERY$
    CREATE INDEX IF NOT EXISTS q_%s_vt_idx ON pgmq.q_%s (vt ASC);
    $QUERY$,
            queue_name, queue_name
            );

    EXECUTE FORMAT(
            $QUERY$
    CREATE INDEX IF NOT EXISTS archived_at_idx_%s ON pgmq.a_%s (archived_at);
    $QUERY$,
            queue_name, queue_name
            );

    EXECUTE FORMAT(
            $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES ('%s', false, false)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
            queue_name
            );
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION pgmq.create_unlogged(queue_name TEXT)
    RETURNS void AS $$
BEGIN
    PERFORM pgmq.validate_queue_name(queue_name);

    EXECUTE FORMAT(
            $QUERY$
    CREATE UNLOGGED TABLE IF NOT EXISTS pgmq.q_%s (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB
    )
    $QUERY$,
            queue_name
            );

    EXECUTE FORMAT(
            $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.a_%s (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB
    );
    $QUERY$,
            queue_name
            );

--     IF NOT pgmq._belongs_to_pgmq(FORMAT('q_%s', queue_name)) THEN
--         EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.q_%s', queue_name);
--     END IF;
--
--     IF NOT pgmq._belongs_to_pgmq(FORMAT('a_%s', queue_name)) THEN
--         EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.a_%s', queue_name);
--     END IF;

    EXECUTE FORMAT(
            $QUERY$
    CREATE INDEX IF NOT EXISTS q_%s_vt_idx ON pgmq.q_%s (vt ASC);
    $QUERY$,
            queue_name, queue_name
            );

    EXECUTE FORMAT(
            $QUERY$
    CREATE INDEX IF NOT EXISTS archived_at_idx_%s ON pgmq.a_%s (archived_at);
    $QUERY$,
            queue_name, queue_name
            );

    EXECUTE FORMAT(
            $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES ('%s', false, true)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
            queue_name
            );
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION pgmq.create_partitioned(
    queue_name TEXT,
    partition_interval TEXT DEFAULT '10000',
    retention_interval TEXT DEFAULT '100000'
)
    RETURNS void AS $$
DECLARE
    partition_col TEXT;
BEGIN
    PERFORM pgmq.validate_queue_name(queue_name);
    PERFORM pgmq._ensure_pg_partman_installed();
    SELECT pgmq._get_partition_col(partition_interval) INTO partition_col;

    EXECUTE FORMAT(
            $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.q_%s (
        msg_id BIGINT GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB
    ) PARTITION BY RANGE (%s)
    $QUERY$,
            queue_name, partition_col
            );

    EXECUTE FORMAT(
            $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.a_%s (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB
    );
    $QUERY$,
            queue_name
            );

--     IF NOT pgmq._belongs_to_pgmq(FORMAT('q_%s', queue_name)) THEN
--         EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.q_%s', queue_name);
--     END IF;
--
--     IF NOT pgmq._belongs_to_pgmq(FORMAT('a_%s', queue_name)) THEN
--         EXECUTE FORMAT('ALTER EXTENSION pgmq ADD TABLE pgmq.a_%s', queue_name);
--     END IF;

    EXECUTE FORMAT(
            $QUERY$
    SELECT public.create_parent('pgmq.q_%s', '%s', 'native', '%s');
    $QUERY$,
            queue_name, partition_col, partition_interval
            );

    EXECUTE FORMAT(
            $QUERY$
    CREATE INDEX IF NOT EXISTS q_%s_part_idx ON pgmq.q_%s (%s);
    $QUERY$,
            queue_name, queue_name, partition_col
            );

    EXECUTE FORMAT(
            $QUERY$
    CREATE INDEX IF NOT EXISTS archived_at_idx_%s ON pgmq.a_%s (archived_at);
    $QUERY$,
            queue_name, queue_name
            );

    EXECUTE FORMAT(
            $QUERY$
    UPDATE public.part_config
    SET
        retention = '%s',
        retention_keep_table = false,
        retention_keep_index = true,
        automatic_maintenance = 'on'
    WHERE parent_table = 'pgmq.q_%s';
    $QUERY$,
            retention_interval, queue_name
            );

    EXECUTE FORMAT(
            $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES ('%s', true, false)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
            queue_name
            );
END;
$$ LANGUAGE plpgsql;