WITH search1 AS (
    SELECT jsonb_array_elements(pgstac.search(:first_req::text::jsonb)->'features') AS feature
),
search2 AS (
    SELECT jsonb_array_elements(pgstac.search(:second_req::text::jsonb)->'features') AS feature
),
all_pairs AS (
    SELECT
        first.feature->>'id' AS id1,
        second.feature->>'id' AS id2,
        first.feature AS first,
        second.feature AS second
    FROM search1 first, search2 second
    WHERE first.feature->>'id' <> second.feature->>'id'
    {filter_expr}
),
limited_pairs AS (
    SELECT id1, id2, first, second
    FROM all_pairs
    OFFSET 0
    LIMIT :limit
),
all_features AS (
    SELECT first AS feature FROM limited_pairs
    UNION
    SELECT second AS feature FROM limited_pairs
),
first_features AS (
    SELECT first AS feature FROM limited_pairs
),
second_features AS (
    SELECT second AS feature FROM limited_pairs
)
SELECT
    CASE
        WHEN :response_type = 'pair' THEN
            jsonb_build_object(
                'type', 'FeatureCollection',
                'featurePairs', (SELECT jsonb_agg(jsonb_build_array(id1, id2)) FROM limited_pairs),
                'features', (SELECT jsonb_agg(feature) FROM all_features),
                'links', '[]'::jsonb,
                'numberReturned', (SELECT count(*) FROM all_features),
                'numberPairsReturned', (SELECT count(*) FROM limited_pairs),
                'numberPairsMatched', (SELECT count(*) FROM all_pairs)
            )
        WHEN :response_type = 'first-only' THEN
            jsonb_build_object(
                'type', 'FeatureCollection',
                'features', (SELECT jsonb_agg(feature) FROM first_features),
                'links', '[]'::jsonb,
                'numberReturned', (SELECT count(*) FROM first_features)
            )
        WHEN :response_type = 'second-only' THEN
            jsonb_build_object(
                'type', 'FeatureCollection',
                'features', (SELECT jsonb_agg(feature) FROM second_features),
                'links', '[]'::jsonb,
                'numberReturned', (SELECT count(*) FROM second_features)
            )
        ELSE
            jsonb_build_object('error', 'Invalid response_type')
    END;