{{ config(materialized='view') }}

with workua as (

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
    from {{ ref('stg_workua_jobs_clean') }}

),

dou as (

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
    from {{ ref('stg_dou_jobs_clean') }}

),

ithub as (

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
    from {{ ref('stg_ithub_jobs_clean') }}

),

unioned as (

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
    from workua

    union all

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
    from dou

    union all

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
    from ithub

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
from unioned