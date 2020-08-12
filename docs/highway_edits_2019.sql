-- built from Seth's example queries on AWS blog. Goal is to create a table of adjustments to highways with all of the relevant metadata for connnecting to changesets 

-- select out nodes and relevant columns
WITH nodes AS (
  SELECT
    type,
    id,
    lat,
    lon,
    changeset
  FROM planet_history
  WHERE type = 'node'
    AND version > 1
    AND "timestamp" > date '2019-01-01'
),

-- select out ways and relevant columns
ways AS (
  SELECT
    type,
    id,
    tags,
    nds
  FROM planet_history
  WHERE type = 'way'
    AND version > 1
    AND tags['highway'] is not null
    AND "timestamp" > date '2019-01-01'
)

SELECT
  ways.type,
  ways.id,
  ways.tags,
  ARRAY[AVG(nodes.lon) lon, AVG(nodes.lat)] coordinates,
  ARRAY_AGG(nodes.changeset) changeset_ids
FROM ways
CROSS JOIN UNNEST(nds) AS t (nd)
JOIN nodes ON nodes.id = nd.ref
GROUP BY (ways.type, ways.id, ways.tags)
