WITH search1 AS (
    SELECT jsonb_array_elements(pgstac.search(:first_req::text::jsonb)->'features') AS feature
),
search2 AS (
    SELECT jsonb_array_elements(pgstac.search(:second_req::text::jsonb)->'features') AS feature
),
all_pairs AS (
-- Create all possible pairs, selecting individual features and their IDs
SELECT
    s1.feature->>'id' AS id1,
    s2.feature->>'id' AS id2,
    s1.feature AS feature1,
    s2.feature AS feature2
FROM search1 s1, search2 s2
WHERE s1.feature->>'id' <> s2.feature->>'id'
),
limited_pairs AS (
-- Apply the user-defined limit to the generated pairs
SELECT id1, id2, feature1, feature2
FROM all_pairs
LIMIT :limit::integer
),
all_features AS (
-- Collect only the unique features that are part of the limited pairs
    SELECT feature1 AS feature FROM limited_pairs
    UNION -- UNION automatically selects distinct features
    SELECT feature2 AS feature FROM limited_pairs
)
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'featurePairs', (SELECT jsonb_agg(jsonb_build_array(id1, id2)) FROM limited_pairs),
    'features', (SELECT jsonb_agg(feature) FROM all_features),
    'links', '[]'::jsonb,
    'numberReturned', (SELECT count(*) FROM all_features),
    'numberPairsReturned', (SELECT count(*) FROM limited_pairs),
    'numberPairsMatched', (SELECT count(*) FROM all_pairs)
) AS result;