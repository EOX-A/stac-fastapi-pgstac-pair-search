WITH search1 AS (
    -- Perform the first search
    SELECT jsonb_array_elements(pgstac.search(:first_req::text::jsonb)->'features') AS feature
),
search2 AS (
    -- Perform the second search
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
limited_second_features AS (
    SELECT feature2 AS feature FROM all_pairs LIMIT :limit::integer
)
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', (SELECT jsonb_agg(feature) FROM limited_second_features),
    'links', '[]'::jsonb,
    'numberReturned', (SELECT count(*) FROM limited_second_features),
    'numberMatched', (SELECT count(*) FROM all_pairs)
) AS result;