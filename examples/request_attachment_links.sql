{{ config(
    materialized="table",
    indexes=[
        {"columns": ["request_id"]},
        {"columns": ["owner_id"]},
        {"columns": ["workspace_id"]}
    ]
  )
}}

WITH files AS (
    SELECT *
    FROM raw.file_attachments
    WHERE owner_type = 'Request'
),

requests AS (
    SELECT
        request_id,
        request_slug,
        workspace_id,
        workspace_slug,
        owner_id,
        owner_email
    FROM curated.requests
),

joined_data AS (
    SELECT
        r.request_id,
        r.request_slug,
        r.workspace_id,
        r.workspace_slug,
        r.owner_id,
        r.owner_email,
        f.file_name,
        f.file_size_bytes,
        f.updated_at AS file_updated_at,
        CONCAT('https://', r.workspace_slug, '.example.com/files/', f.file_uuid) AS file_url
    FROM requests r
    JOIN files f
      ON r.request_id = f.owner_id
)

SELECT * FROM joined_data;
