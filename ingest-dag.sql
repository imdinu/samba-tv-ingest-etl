create or replace task SAMBA_TV.RAW.STB_INGEST
    warehouse = 'S3_INGESTION_WH'
    schedule = 'using cron 4 0 * * * America/New_York'
as copy into SAMBA_TV.RAW.STB
    from (
        select 
            nullif(
              regexp_replace (
              metadata$filename,
              '.*\\/yyyy=(\\d{4})\\/.*',
              '\\1'),
              '__HIVE_DEFAULT_PARTITION__'
            ) AS yyyy,
            nullif(
              regexp_replace (
              metadata$filename,
              '.*\\/mm=(\\d{2})\\/.*',
              '\\1'),
              '__HIVE_DEFAULT_PARTITION__'
            ) AS mm,
            nullif(
              regexp_replace (
              metadata$filename,
              '.*\\/dd=(\\d{2})\\/.*',
              '\\1'),
              '__HIVE_DEFAULT_PARTITION__'
            ) AS dd
            , try_to_date(yyyy || '-' || mm || '-' || dd) AS metadata_date
            , $1:affiliate_call_sign::TEXT AS affiliate_call_sign
            , $1:channel_content_offset_s::NUMBER(38, 0) AS channel_content_offset_s
            , $1:content_id::TEXT AS content_id
            , $1:content_type::TEXT AS content_type
            , $1:description::TEXT AS description
            , $1:dma::TEXT AS dma
            , $1:duration::NUMBER(38, 0) AS duration
            , $1:episode::TEXT AS episode
            , $1:episode_title::TEXT AS episode_title
            , $1:exposure_end_ts::NUMBER(38, 0) AS exposure_end_ts
            , $1:exposure_start_ts::NUMBER(38, 0) AS exposure_start_ts
            , $1:genres::TEXT AS genres
            , $1:intermediate_id::TEXT AS intermediate_id
            , $1:network::TEXT AS network
            , $1:network_id::TEXT AS network_id
            , $1:program_content_offset_s::NUMBER(38, 0) AS program_content_offset_s
            , $1:scheduled_program_end_ts::NUMBER(38, 0) AS scheduled_program_end_ts
            , $1:scheduled_program_start_ts::NUMBER(38, 0) AS scheduled_program_start_ts
            , $1:season::TEXT AS season
            , $1:smba_id::TEXT AS smba_id
            , $1:title::TEXT AS title
            , $1:zip::TEXT AS zip
        from @SAMBA_TV.RAW.SAMBA_S3)
    file_format = SAMBA_TV.RAW.PARQUET_AUTO
    pattern = 'STB/yyyy=\\d{4}/mm=\\d{2}/dd=\\d{2}/.*.parquet$';


create or replace task SAMBA_TV.RAW.ACR_INGEST
    warehouse = 'S3_INGESTION_WH'
    schedule = 'using cron 4 0 * * * America/New_York'
as copy into SAMBA_TV.RAW.ACR
    from (
        select 
            $1:smba_id::text as smba_id
            , $1:intermediate_id::text as intermediate_id
            , $1:exposure_start_ts::number(38, 0) as exposure_start_ts
            , $1:exposure_end_ts::number(38, 0) as exposure_end_ts
            , $1:duration::number(38, 0) as duration
            , $1:content_type::text as content_type
            , $1:content_id::text as content_id
            , $1:title::text as title
            , $1:episode_title::text as episode_title
            , $1:season::text as season
            , $1:episode::text as episode
            , $1:description::text as description
            , $1:genres::text as genres
            , $1:release_date::date as release_date
            , $1:network::text as network
            , $1:network_id::text as network_id
            , $1:affiliate_call_sign::text as affiliate_call_sign
            , $1:scheduled_program_start_ts::number(38, 0) as scheduled_program_start_ts
            , $1:scheduled_program_end_ts::number(38, 0) as scheduled_program_end_ts
            , $1:channel_content_offset_s::number(38, 0) as channel_content_offset_s
            , $1:program_content_offset_s::number(38, 0) as program_content_offset_s
            , $1:application::text as application
            , $1:dma::text as dma
            , $1:zip::text as zip
            , nullif(
              regexp_replace (
              metadata$filename,
              '.*\\/yyyy=(\\d{4})\\/.*',
              '\\1'),
              '__HIVE_DEFAULT_PARTITION__'
            ) as yyyy
            , nullif(
                regexp_replace (
                metadata$filename,
                '.*\\/mm=(\\d{2})\\/.*',
                '\\1'),
                '__HIVE_DEFAULT_PARTITION__'
              ) as mm
            , nullif(
                regexp_replace (
                metadata$filename,
                '.*\\/dd=(\\d{2})\\/.*',
                '\\1'),
                '__HIVE_DEFAULT_PARTITION__'
              ) as dd
            , try_to_date(yyyy || '-' || mm || '-' || dd) as metadata_date
        from @SAMBA_TV.RAW.SAMBA_S3)
    file_format = SAMBA_TV.RAW.PARQUET_AUTO
    pattern = 'ACR/yyyy=\\d{4}/mm=\\d{2}/dd=\\d{2}/.*.parquet$';

create or replace task SAMBA_TV.RAW.GEO_WEIGHTS_INGEST
    warehouse = 'S3_INGESTION_WH'
    schedule = 'using cron 4 0 * * * America/New_York'
as copy into SAMBA_TV.RAW.GEO_WEIGHTS
from (
    select 
        nullif(
          regexp_replace (
          metadata$filename,
          '.*\\/yyyy=(\\d{4})\\/.*',
          '\\1'),
          '__HIVE_DEFAULT_PARTITION__'
        ) AS yyyy,
        nullif(
          regexp_replace (
          metadata$filename,
          '.*\\/mm=(\\d{2})\\/.*',
          '\\1'),
          '__HIVE_DEFAULT_PARTITION__'
        ) AS mm,
        nullif(
          regexp_replace (
          metadata$filename,
          '.*\\/dd=(\\d{2})\\/.*',
          '\\1'),
          '__HIVE_DEFAULT_PARTITION__'
        ) AS dd
        , try_to_date(yyyy || '-' || mm || '-' || dd) AS metadata_date
        , $1:hh_id::TEXT AS hh_id
        , $1:smba_id::TEXT AS smba_id
        , $1:geo_weight::FLOAT AS geo_weight
    from @SAMBA_TV.RAW.SAMBA_S3)
file_format = SAMBA_TV.RAW.PARQUET_AUTO
pattern = 'Geo-Weights/yyyy=\\d{4}/mm=\\d{2}/dd=\\d{2}/.*.parquet$';
    
-- ENABLE TASKS
alter task SAMBA_TV.RAW.STB_INGEST resume;
alter task SAMBA_TV.RAW.ACR_INGEST resume;
alter task SAMBA_TV.RAW.GEO_WEIGHTS_INGEST resume;

execute task SAMBA_TV.RAW.STB_INGEST;
execute task SAMBA_TV.RAW.ACR_INGEST;
execute task SAMBA_TV.RAW.GEO_WEIGHTS_INGEST;
