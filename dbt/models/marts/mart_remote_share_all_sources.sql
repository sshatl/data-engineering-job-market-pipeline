{{ config(materialized='table') }}

with base as (

    select
        coalesce(remote_type, 'unknown') as remote_type
    from {{ ref('int_job_posts_deduped_all_sources') }}

),

agg as (

    select
        remote_type,
        count(*) as job_count
    from base
    group by remote_type

),

totals as (

    select sum(job_count) as total_jobs
    from agg

)

select
    a.remote_type,
    a.job_count,
    round(100.0 * a.job_count / nullif(t.total_jobs, 0), 2) as share_pct
from agg a
cross join totals t
order by a.job_count desc, a.remote_type asc