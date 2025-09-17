WITH search1 AS (
    -- Perform the first search
    SELECT jsonb_array_elements(pgstac.search(:first_req::text::jsonb)->'features') AS feature
),
search2 AS (
    -- Perform the second search
    SELECT jsonb_array_elements(pgstac.search(:second_req::text::jsonb)->'features') AS feature
),
second_features_of_pairs AS (
    -- Find all unique features from the second search that have at least
    -- one non-identical partner in the first search. The EXISTS clause
    -- is efficient as it stops searching as soon as a match is found.
    SELECT s2.feature
    FROM search2 s2
    WHERE EXISTS (
        SELECT 1
        FROM search1 s1
        WHERE s2.feature->>'id' <> s1.feature->>'id'
    )
),
limited_features AS (
    -- Apply the user-defined limit to the features found
    SELECT feature
    FROM second_features_of_pairs
    LIMIT :limit::integer
)
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', (SELECT jsonb_agg(feature) FROM limited_features),
    'links', '[]'::jsonb,
    'numberReturned', (SELECT count(*) FROM limited_features),
    'numberMatched', (SELECT count(*) FROM second_features_of_pairs)
) AS result;