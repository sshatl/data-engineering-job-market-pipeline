{{ config(materialized='table') }}

with base as (

    select
        coalesce(seniority, 'unknown') as seniority
    from {{ ref('int_job_posts_deduped_all_sources') }}

),

agg as (

    select
        seniority,
        count(*) as job_count
    from base
    group by seniority

),

totals as (

    select sum(job_count) as total_jobs
    from agg

)

select
    a.seniority,
    a.job_count,
    round(100.0 * a.job_count / nullif(t.total_jobs, 0), 2) as share_pct
from agg a
cross join totals t
order by a.job_count desc, a.seniority asc