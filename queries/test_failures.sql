WITH
    -- Get all PRs that were merged into master, and get all the SHAs for commits
    -- from that PR which CI jobs ran against.
    pr_shas AS (
        SELECT
            r.pull_requests[1].number AS pr_number,
            j.head_sha AS sha,
        FROM
            commons.workflow_job j
            INNER JOIN commons.workflow_run r on j.run_id = r.id
        WHERE
            1 = 1
            AND j._event_time > (
                CURRENT_DATETIME() - DAYS(:days_back_start + :num_days_to_cover)
            )
            AND r._event_time > (
                CURRENT_DATETIME() - DAYS(:days_back_start + :num_days_to_cover)
            )
            AND j._event_time < (CURRENT_DATETIME() - DAYS(:days_back_start))
            AND r._event_time < (CURRENT_DATETIME() - DAYS(:days_back_start))
            AND LENGTH(r.pull_requests) = 1 -- Some jobs have many PRs associated with them. They don't offer much of a signal to us
            AND r.head_branch NOT IN ('master', 'main', 'nightly')
            AND r.pull_requests[1].head.repo.name = 'pytorch'
            AND r.name IN ('pull', 'trunk', 'Lint', 'periodic')
            AND (
                -- Ensure we don't pull in random PRs we don't care about
                r.pull_requests[1].base.ref in ('master', 'main')
                OR r.pull_requests[1].base.ref like 'gh/%/base'
            )
        GROUP BY
            pr_number,
            sha
    ),
    -- Get all the workflows and partially aggregate the jobs run against
    -- each commit (based on the job's conclusion)
    test_failures AS (
        SELECT
            s.pr_number,
            s.sha,
            f.failure,
            f.invoking_file,
            f.classname,
            f.file as test_file,
            min(j._event_time) AS start_time,
            max(PARSE_TIMESTAMP_ISO8601(j.completed_at)) AS end_time,
        FROM
            commons.workflow_job j
            INNER JOIN pr_shas s on j.head_sha = s.sha
            INNER JOIN commons.workflow_run r on j.run_id = r.id
            INNER JOIN commons.failed_tests_run f on f.job_id = j.id
        WHERE
            1 = 1
            AND j._event_time > (
                CURRENT_DATETIME() - DAYS(:days_back_start + :num_days_to_cover)
            )
            AND j._event_time < (CURRENT_DATETIME() - DAYS(:days_back_start))
            AND (
                r.name IN ('pull', 'trunk', 'Lint', 'periodic')
                OR r.name like 'linux-binary%'
                OR r.name like 'windows-binary%'
            )
            AND j.conclusion NOT IN ('skipped')
            AND f.failure is not NULL
        GROUP BY
            pr_number,
            sha,
            classname,
            failure,
            invoking_file,
            test_file
    )
SELECT
    *
FROM
    test_failures
order by sha