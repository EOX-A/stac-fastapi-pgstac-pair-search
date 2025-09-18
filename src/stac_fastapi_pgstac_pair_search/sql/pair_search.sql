CREATE OR REPLACE FUNCTION pair_search(
    first_req text,
    second_req text,
    _limit int DEFAULT 10,
    response_type text DEFAULT 'pair'
) RETURNS jsonb AS $$
DECLARE
    result jsonb;
BEGIN
    WITH search1 AS (
        SELECT jsonb_array_elements(pgstac.search(first_req::text::jsonb)->'features') AS feature
    ),
    search2 AS (
        SELECT jsonb_array_elements(pgstac.search(second_req::text::jsonb)->'features') AS feature
    ),
    all_pairs AS (
        SELECT
            s1.feature->>'id' AS id1,
            s2.feature->>'id' AS id2,
            s1.feature AS feature1,
            s2.feature AS feature2
        FROM search1 s1, search2 s2
        WHERE s1.feature->>'id' <> s2.feature->>'id'
    ),
    limited_pairs AS (
        SELECT id1, id2, feature1, feature2
        FROM all_pairs
        WHERE TRUE
        OFFSET 0
        LIMIT _limit
    ),
    all_features AS (
        SELECT feature1 AS feature FROM limited_pairs
        UNION
        SELECT feature2 AS feature FROM limited_pairs
    ),
    first_features AS (
        SELECT feature1 AS feature FROM limited_pairs
    ),
    second_features AS (
        SELECT feature2 AS feature FROM limited_pairs
    )
    SELECT
        CASE
            WHEN response_type = 'pair' THEN
                jsonb_build_object(
                    'type', 'FeatureCollection',
                    'featurePairs', (SELECT jsonb_agg(jsonb_build_array(id1, id2)) FROM limited_pairs),
                    'features', (SELECT jsonb_agg(feature) FROM all_features),
                    'links', '[]'::jsonb,
                    'numberReturned', (SELECT count(*) FROM all_features),
                    'numberPairsReturned', (SELECT count(*) FROM limited_pairs),
                    'numberPairsMatched', (SELECT count(*) FROM all_pairs)
                )
            WHEN response_type = 'first-only' THEN
                jsonb_build_object(
                    'type', 'FeatureCollection',
                    'features', (SELECT jsonb_agg(feature) FROM first_features),
                    'links', '[]'::jsonb,
                    'numberReturned', (SELECT count(*) FROM first_features)
                )
            WHEN response_type = 'second-only' THEN
                jsonb_build_object(
                    'type', 'FeatureCollection',
                    'features', (SELECT jsonb_agg(feature) FROM second_features),
                    'links', '[]'::jsonb,
                    'numberReturned', (SELECT count(*) FROM second_features)
                )
            ELSE
                jsonb_build_object('error', 'Invalid response_type')
        END
    INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql STABLE;