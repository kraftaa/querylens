WITH entity_features AS (
    SELECT
        entity_id,
        feature_slug,
        status,
        updated_at
    FROM curated.feature_registry
),

entity_activity AS (
    SELECT
        entity_id,
        COUNT(DISTINCT workflow_id) AS workflows_seen,
        COUNT(DISTINCT workflow_id) FILTER (WHERE completed_at IS NOT NULL) AS workflows_completed
    FROM curated.workflow_events
    GROUP BY entity_id
),

entity_catalog AS (
    SELECT
        entity_id,
        ARRAY_AGG(DISTINCT category_name) FILTER (WHERE category_name IS NOT NULL) AS categories,
        ARRAY_AGG(DISTINCT tag_name) FILTER (WHERE tag_name IS NOT NULL) AS tags
    FROM curated.entity_catalog_items
    GROUP BY entity_id
)

SELECT
    e.entity_id,
    e.entity_name,
    a.workflows_seen,
    a.workflows_completed,
    c.categories,
    c.tags,
    MAX(f.updated_at) AS last_feature_update
FROM curated.entities e
LEFT JOIN entity_activity a ON a.entity_id = e.entity_id
LEFT JOIN entity_catalog c ON c.entity_id = e.entity_id
LEFT JOIN entity_features f ON f.entity_id = e.entity_id
GROUP BY
    e.entity_id,
    e.entity_name,
    a.workflows_seen,
    a.workflows_completed,
    c.categories,
    c.tags;
