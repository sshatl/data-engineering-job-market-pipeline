{{ config(materialized='table') }}

with base as (

    select
        cross_source_job_key,
        skills
    from {{ ref('int_job_posts_deduped_all_sources') }}
    where coalesce(nullif(trim(skills), ''), '') <> ''

),

exploded as (

    select
        cross_source_job_key,
        trim(skill) as skill
    from base,
    lateral unnest(string_to_array(skills, ',')) as skill

),

cleaned as (

    select distinct
        cross_source_job_key,
        skill
    from exploded
    where coalesce(nullif(trim(skill), ''), '') <> ''

)

select
    skill,
    count(*) as job_count
from cleaned
group by skill
order by job_count desc, skill asc