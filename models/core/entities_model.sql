-- models/core/entities.sql

SELECT
  *
FROM {{ ref('dim_entities') }}
