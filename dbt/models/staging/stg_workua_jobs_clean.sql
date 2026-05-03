{{ config(
    materialized='incremental',
    unique_key='source_job_uid'
) }}

with source_data as (

    select
        cast(source_job_uid as text) as source_job_uid,
        cast(job_id as text) as job_id,
        cast(job_url as text) as job_url,
        cast(title as text) as title,
        cast(company as text) as company,
        cast(location as text) as location,
        cast(snippet as text) as snippet,
        cast(published_text as text) as published_text,
        cast(description_full as text) as description_full,
        cast(page_text as text) as page_text,
        cast(source as text) as source,
        cast(query_name as text) as query_name,
        cast(query_text as text) as query_text,
        cast(role_family as text) as role_family,
        cast(remote_type as text) as remote_type,
        cast(seniority as text) as seniority,
        cast(skills as text) as skills,
        cast(dt as text) as dt,
        cast(fetched_at as text) as fetched_at
    from {{ source('raw_jobs', 'workua_jobs_clean') }}
    {% if is_incremental() %}
    where dt > (select max(dt) from {{ this }})
    {% endif %}

),

deduped as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by source_job_uid
                order by
                    fetched_at desc nulls last,
                    dt desc nulls last,
                    job_url desc nulls last
            ) as rn
        from source_data
    ) t
    where rn = 1

)

select
    source_job_uid,
    job_id,
    job_url,
    title,
    company,
    location,
    snippet,
    published_text,
    description_full,
    page_text,
    source,
    query_name,
    query_text,
    role_family,
    remote_type,
    seniority,
    skills,
    dt,
    fetched_at
from deduped