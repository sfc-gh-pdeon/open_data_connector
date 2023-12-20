//This task is intended to update URLs every week to make sure they are still accessible. This is regardless of data updates.
CREATE OR REPLACE task core.refresh_urls_task
 SCHEDULE = '10078 MINUTE' --7 DAYS - 2 minutes to be just under the presigned url expiration
 WAREHOUSE = {0}
 AS
  EXECUTE IMMEDIATE
 $$
    BEGIN
        INSERT INTO ckan_log
        SELECT current_timestamp()
        ,rs.package_id
        ,parse_json(config.resource_update(rs.resource_id,rs.extension,rs.presigned_url)):id::string ext_resource_id
        ,rs.table_name,'presigned url updated at CKAN'        
        FROM core.resources rs;
    EXCEPTION
    WHEN OTHER THEN
        let err string := SQLERRM;
        INSERT INTO ckan_log
        select current_timestamp(),rs.package_id,rs.resource_id,rs.table_name,'presigned url update failed: ' || :err
        FROM core.resources rs;
    END;
$$;

GRANT ALL ON TASK core.refresh_urls_task TO APPLICATION ROLE ckan_app_role;
alter task core.refresh_urls_task resume;    