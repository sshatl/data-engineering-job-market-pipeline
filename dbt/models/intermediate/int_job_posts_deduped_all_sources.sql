{{ config(materialized='view') }}

with base as (

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
        fetched_at,

        lower(trim(regexp_replace(coalesce(title, ''), '[^a-zA-Zа-яА-ЯіїєґІЇЄҐ0-9]+', ' ', 'g'))) as norm_title,
        lower(trim(regexp_replace(coalesce(company, ''), '[^a-zA-Zа-яА-ЯіїєґІЇЄҐ0-9]+', ' ', 'g'))) as norm_company,

        case
            when coalesce(nullif(trim(skills), ''), '') <> '' then 1
            else 0
        end as has_skills,

        length(coalesce(description_full, '')) as description_len,

        case
            when source = 'dou' then 1
            when source = 'ithub' then 2
            when source = 'workua' then 3
            else 9
        end as source_priority
    from {{ ref('int_job_posts_all_sources') }}

),

with_keys as (

    select
        *,
        md5(
            coalesce(norm_company, '') || '|' ||
            coalesce(norm_title, '') || '|' ||
            coalesce(seniority, 'unknown')
        ) as cross_source_job_key
    from base

),

ranked as (

    select
        *,
        row_number() over (
            partition by cross_source_job_key
            order by
                has_skills desc,
                description_len desc,
                source_priority asc,
                fetched_at desc nulls last,
                dt desc nulls last,
                source_job_uid desc
        ) as rn
    from with_keys

)

select
    cross_source_job_key,
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
from ranked
where rn = 1