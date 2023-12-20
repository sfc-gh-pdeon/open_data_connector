//this is intended to capture any updates as they occur on the resources table. i.e. data has been update in the base table
CREATE OR REPLACE task core.refresh_updated_urls_task
 SCHEDULE = 'USING CRON 0 23 * * * America/Los_Angeles'
 WAREHOUSE = {0}
 AS
 EXECUTE IMMEDIATE
 $$
    BEGIN
        --Get all Databases that we can see so that we can find each tables last altered data. Exclude the app itself.
        SHOW TERSE DATABASES IN ACCOUNT; 
        LET dbs RESULTSET := (SELECT "name" db_name FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) res WHERE db_name <> current_database()); 

        --loop through each db and build the query to get the last_altered datetime.
        FOR t IN dbs DO
            IF (is_first) THEN
                sql := 'SELECT LAST_ALTERED,table_catalog db_name, table_Schema schema_name, table_name FROM '||t.db_name||'.INFORMATION_SCHEMA."TABLES" WHERE schema_name <> \'INFORMATION_SCHEMA\' ';
                is_first := false;
            ELSE
                sql := sql || ' UNION ALL SELECT LAST_ALTERED,table_catalog db_name, table_Schema schema_name, table_name FROM '||t.db_name||'.INFORMATION_SCHEMA."TABLES" WHERE schema_name <> \'INFORMATION_SCHEMA\'';
            END IF;
        END FOR;

        --Invalidate the presigned_url on the resource table for any table that was updated in the last 24 hours. 
        --This will force records into the resource_stream
        sql := 'UPDATE core.resources
                set presigned_url = null
                where exists (select 1 from ('||:sql||') info_schema 
                    where info_schema.db_name = core.resources.database_name
                    and info_schema.schema_name = core.resources.schema_name
                    and info_schema.table_name = core.resources.table_name
                    and last_altered > dateadd(hours,-24,current_time())
                    )';

        execute immediate :sql;
       
        --Unload all files that are in the resouces_Stream and publish to CKAN
        CALL CONFIG.SP_UPDATE_RESOURCES();
    EXCEPTION
    WHEN OTHER THEN
        let err string := SQLERRM;
        INSERT INTO core.ckan_log
        select current_timestamp(),rs.package_id,rs.resource_id,rs.table_name,'presigned url update failed: ' || :err
        FROM core.resources_stream rs;
    END;
$$;

GRANT ALL ON TASK core.refresh_updated_urls_task TO APPLICATION ROLE ckan_app_role;
alter task core.refresh_updated_urls_task resume;    