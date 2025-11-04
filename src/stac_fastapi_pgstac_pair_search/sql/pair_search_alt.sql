-- alternative version of the pair search
CREATE OR REPLACE FUNCTION pair_search_alt(
    query jsonb = '{}'::jsonb
) RETURNS jsonb AS $$
DECLARE
    temp record;
    parameters record;
    response_type text;
    result json;
    conf jsonb := coalesce(query->'conf', '{}'::jsonb);
    hydrate bool := NOT (conf->>'nohydrate' IS NOT NULL AND (conf->>'nohydrate')::boolean = true);
    _limit int;
    _query_limit int;
    _offset int := NULL;
    _where text := NULL;
    _fields jsonb := coalesce(query->'fields', '{}'::jsonb);
    _items jsonb;
    pair_ids jsonb;
    links jsonb := '[]'::jsonb;
    total_timer timestamptz := clock_timestamp();
    timer timestamptz;
    has_prev bool := FALSE;
    has_next bool := FALSE;
    item_count int;
    pair_count int;
    next_offset int;
    prev_offset int;
BEGIN
    -- TODO token pagination
    -- TODO fields selection
    -- TODO result ordering

    timer := clock_timestamp();

    parameters := _parse_common_parameters(query);
    _limit := parameters._limit;
    _offset := parameters._offset;
    response_type := parameters.response_type;
    _where := _pair_search_query_to_where(query);

    IF _limit < 1 THEN
        _query_limit := 0;
    ELSE
        _query_limit := _limit + 1;
    END IF;

    RAISE NOTICE 'pair-search[%]: Time to parse inputs: %ms', response_type, pgstac.age_ms(timer);
    timer := clock_timestamp();

    IF response_type = 'pair' THEN
          WITH
              pair_records AS (
                  SELECT * FROM search_item_pairs(_where, _query_limit, _offset)
              ),
              pair_ids AS (
                  SELECT (first).id AS first_id, (second).id AS second_id FROM pair_records
              ),
              pair_items_all AS (
                  (SELECT first AS item FROM pair_records LIMIT _limit)
                  UNION
                  (SELECT second AS item FROM pair_records LIMIT _limit)
              ),
              pair_items_distint AS (
                  SELECT DISTINCT ON ((item).id) item FROM pair_items_all
              )
          SELECT
              (SELECT jsonb_agg(jsonb_build_array(first_id, second_id)) FROM pair_ids) AS pair_ids,
              (SELECT jsonb_agg(format_item(item, _fields, _hydrated => hydrate)) FROM pair_items_distint) AS pair_items
        INTO temp;

        _items := coalesce(temp.pair_items, '[]'::jsonb);
        pair_ids := coalesce(temp.pair_ids, '[]'::jsonb);

        RAISE NOTICE 'pair-search[%s]: Time to fetch items: %ms', response_type, pgstac.age_ms(timer);
        timer := clock_timestamp();

        -- remove extra records and resolve pagination
        item_count := jsonb_array_length(_items);
        pair_count := jsonb_array_length(pair_ids);
        _offset = coalesce(_offset, 0);
        IF pair_count > _limit THEN
            pair_ids := pair_ids - -1;
            pair_count := pair_count - 1;
            has_next := TRUE;
            next_offset := _offset + _limit;
        END IF;

    ELSIF response_type IN ('first-only', 'second-only') THEN

        SELECT jsonb_agg(format_item(item_records, _fields, _hydrated => hydrate))
        INTO _items
        FROM search_item_single(
            _where, _query_limit, _offset,
            CASE response_type
                WHEN 'first-only' THEN 'first'
                WHEN 'second-only' THEN 'second'
            END
        ) AS item_records;

        _items := coalesce(_items, '[]'::jsonb);

        RAISE NOTICE 'pair-search[%s]: Time to fetch items: %ms', response_type, pgstac.age_ms(timer);
        timer := clock_timestamp();

        -- remove extra records and resolve pagination
        _offset = coalesce(_offset, 0);
        item_count := jsonb_array_length(_items);
        IF item_count > _limit THEN
            _items := _items - -1;
            item_count := item_count - 1;
            has_next := TRUE;
            next_offset := _offset + _limit;
        END IF;

    ELSE
        RAISE EXCEPTION 'Invalid response type! %', response_type;
    END IF;

    IF _offset > 0 THEN
        has_prev := TRUE;
        prev_offset := GREATEST(0, _offset - _limit);
    END IF;

    -- build links - links are expected to be refined by the front-end
    IF has_next THEN
        links := links || jsonb_build_object(
            'rel', 'next',
            'parameters', jsonb_build_object(
                'offset', next_offset
            )
        );
    END IF;

    IF has_prev THEN
        links := links || jsonb_build_object(
            'rel', 'prev',
            'parameters', jsonb_build_object(
                'offset', prev_offset
            )
        );
    END IF;

    IF response_type = 'pair' THEN
        result := jsonb_build_object(
            'type', 'FeatureCollection',
            'featurePairs', coalesce(pair_ids, '[]'::jsonb),
            'features', coalesce(_items, '[]'::jsonb),
            'links', links,
            'numberReturned', item_count,
            'numberPairsReturned', pair_count
        );
    ELSE
        result := jsonb_build_object(
            'type', 'FeatureCollection',
            'features', coalesce(_items, '[]'::jsonb),
            'links', links,
            'numberReturned', item_count
        );
    END IF;

    IF get_setting_bool('timing', conf) THEN
        result = collection || jsonb_build_object(
            'timing', age_ms(total_timer)
        );
    END IF;

    RAISE NOTICE 'search[%]: Time to build response: %ms', response_type, pgstac.age_ms(timer);

    RETURN result;
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


-- alternative version of the item search
CREATE OR REPLACE FUNCTION search_alt(
    query jsonb = '{}'::jsonb
) RETURNS jsonb AS $$
DECLARE
    parameters record;
    result json;
    links jsonb := '[]'::jsonb;
    conf jsonb := coalesce(query->'conf', '{}'::jsonb);
    hydrate bool := NOT (conf->>'nohydrate' IS NOT NULL AND (conf->>'nohydrate')::boolean = true);
    _limit int := 10;
    _query_limit int;
    _offset int := NULL;
    _where text := NULL;
    _fields jsonb := coalesce(query->'fields', '{}'::jsonb);
    _items jsonb;
    total_timer timestamptz := clock_timestamp();
    timer timestamptz;
    has_prev bool := FALSE;
    has_next bool := FALSE;
    item_count int;
    next_offset int;
    prev_offset int;
BEGIN
    -- TODO token pagination
    -- TODO fields selection
    -- TODO result ordering

    timer := clock_timestamp();

    parameters := _parse_common_parameters(query);
    _limit := parameters._limit;
    _offset := parameters._offset;
    _where := _search_query_to_where(query);

    IF _limit < 1 THEN
        _query_limit := 0;
    ELSE
        _query_limit := _limit + 1;
    END IF;

    RAISE NOTICE 'search: Time to parse inputs: %ms', pgstac.age_ms(timer);
    timer := clock_timestamp();

    SELECT jsonb_agg(format_item(item_records, _fields, _hydrated => hydrate))
    INTO _items
    FROM search_items(_where, _query_limit, _offset) AS item_records;

    _items := coalesce(_items, '[]'::jsonb);

    RAISE NOTICE 'search: Time to fetch items: %ms', pgstac.age_ms(timer);
    timer := clock_timestamp();

    -- remove extra records and resolve pagination
    _offset = coalesce(_offset, 0);
    item_count := jsonb_array_length(_items);
    IF item_count > _limit THEN
        _items := _items - -1;
        item_count := item_count - 1;
        has_next := TRUE;
        next_offset := _offset + _limit;
    END IF;

    IF _offset > 0 THEN
        has_prev := TRUE;
        prev_offset := GREATEST(0, _offset - _limit);
    END IF;

    -- build links - links are expected to be refined by the front-end
    IF has_next THEN
        links := links || jsonb_build_object(
            'rel', 'next',
            'parameters', jsonb_build_object(
                'offset', next_offset
            )
        );
    END IF;

    IF has_prev THEN
        links := links || jsonb_build_object(
            'rel', 'prev',
            'parameters', jsonb_build_object(
                'offset', prev_offset
            )
        );
    END IF;

    result := jsonb_build_object(
        'type', 'FeatureCollection',
        'features', coalesce(_items, '[]'::jsonb),
        'links', links,
        'numberReturned', item_count
    );

    IF get_setting_bool('timing', conf) THEN
        result = collection || jsonb_build_object(
            'timing', age_ms(total_timer)
        );
    END IF;

    RAISE NOTICE 'search: Time to build response: %ms', pgstac.age_ms(timer);

    RETURN result;
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


--- parsing common arguments
CREATE OR REPLACE FUNCTION _parse_common_parameters(
    IN query jsonb,
    IN default_limit int DEFAULT 10,
    IN default_offset int DEFAULT NULL,
    IN default_response_type text DEFAULT 'pair',
    OUT _limit int,
    OUT _offset int,
    OUT response_type text
) AS $$
BEGIN
    IF query ? 'limit' THEN
        IF _is_int(query->>'limit') AND (query->>'limit')::int >= 0 THEN
            _limit := (query->>'limit')::int;
        ELSE
            RAISE EXCEPTION 'Invalid limit value!';
        END IF;
    ELSE
        _limit := default_limit;
    END IF;

    IF query ? 'offset' THEN
        IF _is_int(query->>'offset') AND (query->>'offset')::int >= 0 THEN
            _offset := (query->>'offset')::int;
        ELSE
            RAISE EXCEPTION 'Invalid offset value!';
        END IF;
    ELSE
        _offset := default_offset;
    END IF;

    IF query ? 'response-type' THEN
        response_type := LOWER(query->>'response-type');
    ELSE
        response_type := default_response_type;
    END IF;
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


--pairs search query parsing
CREATE OR REPLACE FUNCTION _pair_search_query_to_where(
    query jsonb
) RETURNS text AS $$
DECLARE
    parts text[] DEFAULT NULL;
    geom geometry;
    filterlang text;
    parameter text;
BEGIN
    IF query ? 'first-ids' THEN
        parts := parts || _ids_to_where(to_text_array(query->'first-ids'), 'first.');
    END IF;

    IF query ? 'second-ids' THEN
        parts := parts || _ids_to_where(to_text_array(query->'second-ids'), 'second.');
    END IF;

    IF query ? 'first-collections' THEN
        parts := parts || _collections_to_where(to_text_array(query->'first-collections'), 'first.');
    END IF;

    IF query ? 'second-collections' THEN
        parts := parts || _collections_to_where(to_text_array(query->'second-collections'), 'second.');
    END IF;

    IF query ? 'first-datetime' THEN
        parts := parts || _datetime_to_where(query->'first-datetime', 'first.');
    END IF;

    IF query ? 'second-datetime' THEN
        parts := parts || _datetime_to_where(query->'second-datetime', 'second.');
    END IF;

    -- handle intersects, geometry, bbox spatial queries
    geom := _extract_stac_geom(query, 'first-');
    IF geom IS NOT NULL THEN
        parts := parts || _geometry_to_where(geom, 'first.');
    END IF;

    geom := _extract_stac_geom(query, 'second-');
    IF geom IS NOT NULL THEN
        parts := parts || _geometry_to_where(geom, 'second.');
    END IF;

    -- handle filter
    IF query ? 'filter' THEN
        filterlang := COALESCE(query->>'filter-lang', 'cql2-json');
        IF filterlang <> 'cql2-json' THEN
            RAISE EXCEPTION '% is not a supported filter-lang.', filterlang;
        END IF;
        parts := parts || cql2_to_sql_predicate(query->'filter', '{first.,second.}');
    END IF;

    IF query ? 'q' THEN
        RAISE EXCEPTION 'Free text search (q parameter) is not supported.';
    END IF;

    FOREACH parameter IN ARRAY '{query,ids,collections,datetime,geometry,bbox,intersects}'::text[] LOOP
        IF query ? parameter THEN
            RAISE EXCEPTION '% parameter is not supported.', parameter;
        END IF;
    END LOOP;

    RETURN array_to_string(array_remove(parts, NULL), ' AND ');
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


-- search query parsing
CREATE OR REPLACE FUNCTION _search_query_to_where(
    query jsonb
) RETURNS text AS $$
DECLARE
    parts text[] DEFAULT NULL;
    geom geometry;
    filterlang text;
BEGIN
    IF query ? 'ids' THEN
        parts := parts || _ids_to_where(to_text_array(query->'ids'));
    END IF;

    IF query ? 'collections' THEN
        parts := parts || _collections_to_where(to_text_array(query->'collections'));
    END IF;

    IF query ? 'datetime' THEN
        parts := parts || _datetime_to_where(query->'datetime');
    END IF;

    -- handle intersects, geometry, bbox spatial queries
    geom := _extract_stac_geom(query);
    IF geom IS NOT NULL THEN
        parts := parts || _geometry_to_where(geom);
    END IF;

    -- handle filter
    IF query ? 'filter' THEN
        filterlang := COALESCE(query->>'filter-lang', 'cql2-json');
        IF filterlang <> 'cql2-json' THEN
            RAISE EXCEPTION '% is not a supported filter-lang.', filterlang;
        END IF;
        parts := parts || cql2_to_sql_predicate(query->'filter');
    END IF;

    IF query ? 'query' THEN
        RAISE EXCEPTION 'query parameter is not supported.';
    END IF;

    IF query ? 'q' THEN
        RAISE EXCEPTION 'Free text search (q parameter) is not supported.';
    END IF;

    RETURN array_to_string(array_remove(parts, NULL), ' AND ');
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


-- Stripped-down version of the item pairs search
CREATE OR REPLACE FUNCTION search_item_pairs(
    IN _where text DEFAULT NULL,
    IN _limit int DEFAULT NULL,
    IN _offset int DEFAULT NULL,
    OUT first pgstac.items,
    OUT second pgstac.items
) RETURNS SETOF record AS $$
DECLARE
    query text;
    extra_clauses text;
    timer timestamptz := clock_timestamp();
    explain_text text;
BEGIN
    extra_clauses := ' WHERE first.id <> second.id';
    IF _where IS NOT NULL AND trim(_where) <> '' THEN
        extra_clauses := format('%s AND (%s)', extra_clauses, _where);
    END IF;
    IF _limit IS NOT NULL AND _limit >= 0 THEN
        extra_clauses := format('%s LIMIT %s', extra_clauses, _limit);
    END IF;
    IF _offset IS NOT NULL AND _offset > 0 THEN
        extra_clauses := format('%s OFFSET %s', extra_clauses, _offset);
    END IF;
    query := format(
        'SELECT first, second'
        ' FROM pgstac.items AS first, pgstac.items AS second %s',
        extra_clauses
    );
    RAISE DEBUG 'QUERY: %', query;

    EXECUTE format('EXPLAIN %s', query) INTO explain_text;
    RAISE DEBUG 'QUERY PLAN: %s', explain_text;
    RETURN QUERY EXECUTE query;
    RAISE NOTICE 'QUERY TOOK %ms', pgstac.age_ms(timer);
RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


-- search first or second distinct pair items
CREATE OR REPLACE FUNCTION search_item_single(
    _where text DEFAULT NULL,
    _limit int DEFAULT NULL,
    _offset int DEFAULT NULL,
    selector text DEFAULT 'first' -- or 'second'
) RETURNS SETOF pgstac.items AS $$
DECLARE
    query text;
    extra_clauses text = '';
    timer timestamptz := clock_timestamp();
    explain_text text;
BEGIN
    IF selector NOT IN ('first', 'second') THEN
        RAISE EXCEPTION 'Invalid item selection! %', selector;
    END IF;
    extra_clauses := ' WHERE first.id <> second.id';
    IF _where IS NOT NULL AND trim(_where) <> '' THEN
        extra_clauses := extra_clauses || format(' AND (%s)', _where);
    END IF;
    IF _limit IS NOT NULL AND _limit >= 0 THEN
        extra_clauses := extra_clauses || format(' LIMIT %s', _limit);
    END IF;
    IF _offset IS NOT NULL AND _offset > 0 THEN
        extra_clauses := extra_clauses || format(' OFFSET %s', _offset);
    END IF;
    query := format(
        'SELECT DISTINCT ON (%s.id) %s.*'
        ' FROM pgstac.items AS first, pgstac.items AS second %s',
        selector, selector,
        extra_clauses
    );
    RAISE DEBUG 'QUERY: %', query;
    EXECUTE format('EXPLAIN %s', query) INTO explain_text;
    RAISE DEBUG 'QUERY PLAN: %s', explain_text;
    RETURN QUERY EXECUTE query;
    RAISE NOTICE 'QUERY TOOK %ms', pgstac.age_ms(timer);
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


-- Stripped-down version of the item search
CREATE OR REPLACE FUNCTION search_items(
    IN _where text DEFAULT NULL,
    IN _limit int DEFAULT NULL,
    IN _offset int DEFAULT NULL
) RETURNS SETOF pgstac.items AS $$
DECLARE
    query text;
    where_clause text = '';
    limit_clause text = '';
    offset_clause text = '';
    timer timestamptz := clock_timestamp();
    explain_text text;
BEGIN
    IF _where IS NOT NULL AND trim(_where) <> '' THEN
         where_clause := format(' WHERE (%s)', _where);
    END IF;
    IF _limit IS NOT NULL AND _limit >= 0 THEN
        limit_clause := format(' LIMIT %s', _limit);
    END IF;
    IF _offset IS NOT NULL AND _offset > 0 THEN
        offset_clause := format(' OFFSET %s', _offset);
    END IF;
    query := format(
        'SELECT * FROM pgstac.items%s%s%s',
        where_clause,
        limit_clause,
        offset_clause
    );
    RAISE DEBUG 'QUERY: %', query;
    EXECUTE format('EXPLAIN %s', query) INTO explain_text;
    RAISE DEBUG 'QUERY PLAN: %s', explain_text;
    RETURN QUERY EXECUTE query;
    RAISE NOTICE 'QUERY TOOK %ms', pgstac.age_ms(timer);
RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


-- Query parsing and conversion to WHERE clause predicates

CREATE OR REPLACE FUNCTION _ids_to_where(
    IN ids text[],
    IN prefix text DEFAULT ''
) RETURNS text AS $$
BEGIN
    RETURN format('%sid = ANY (%L) ', prefix, ids);
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION _collections_to_where(
    IN collections text[],
    IN prefix text DEFAULT ''
) RETURNS text AS $$
BEGIN
    RETURN format('%scollection = ANY (%L) ', prefix, collections);
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION _datetime_to_where(
    IN datetime_range jsonb,
    IN prefix text DEFAULT ''
) RETURNS text AS $$
DECLARE
    parsed_range tstzrange;
    start_datetime timestamptz;
    end_datetime timestamptz;
BEGIN
    parsed_range := parse_dtrange(datetime_range);
    start_datetime := lower(parsed_range);
    end_datetime := upper(parsed_range);
    RETURN format(
        '%sdatetime < %L::timestamptz AND '
        '%send_datetime >= %L::timestamptz',
        prefix, end_datetime,
        prefix, start_datetime
    );
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION _geometry_to_where(
    IN geom geometry,
    IN prefix text DEFAULT ''
) RETURNS text AS $$
BEGIN
    RETURN format('st_intersects(%sgeometry, %L)', prefix, geom);
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION _extract_stac_geom(
    IN value jsonb,
    IN prefix text DEFAULT ''
) RETURNS geometry AS $$
DECLARE
    intersects_key text := format('%sintersects', prefix);
    geometry_key text := format('%sgeometry', prefix);
    bbox_key text := format('%sbbox', prefix);
    geom geometry DEFAULT NULL;
BEGIN
    IF value ? bbox_key THEN
        geom := pgstac.bbox_geom(value->bbox_key);
    ELSIF value ? intersects_key THEN
        geom := ST_GeomFromGeoJSON(value->>intersects_key);
    ELSIF value ? geometry_key THEN
        geom := ST_GeomFromGeoJSON(value->>geometry_key);
    END IF;
    RETURN geom;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;


-- CQL2 to SQL predicate conversions -- loosely based on pgstac.cql2_query
CREATE OR REPLACE FUNCTION cql2_to_sql_predicate(
    query jsonb,
    prefixes text[] DEFAULT NULL,
    allow_extra_properties bool DEFAULT TRUE
) RETURNS text AS $$
-- #variable_conflict use_variable
DECLARE
    operator text := lower(query->>'op');
    arguments jsonb := query->'args';
    argument jsonb;
    property text;
    wrapper text;
    template text;
    queryable record;
BEGIN
    IF query IS NULL OR (operator IS NOT NULL AND arguments IS NULL) THEN
        RETURN NULL;
    END IF;

    RAISE NOTICE 'CQL2 QUERY: %', query;

    IF NOT allow_extra_properties THEN
        CALL _enforce_queryables(query, prefixes);
    END IF;

    IF query ? 'filter' THEN
        RETURN cql2_to_sql_predicate(query->'filter', prefixes);
    END IF;

    -- lower and upper operators
    IF query ? 'upper' THEN
        RETURN cql2_to_sql_predicate(
            jsonb_build_object('op', 'upper', 'args', query->'upper'),
            prefixes
        );
    END IF;

    IF query ? 'lower' THEN
        RETURN cql2_to_sql_predicate(
            jsonb_build_object('op', 'lower', 'args', query->'lower'),
            prefixes
        );
    END IF;

    -- handling temporal operators
    IF operator ^@ 't_' or operator = 'anyinteracts' THEN
        RETURN _temporal_operators(query, prefixes);
    END IF;

    -- handling temporal literals
    IF (
        query ? 'timestamp' OR query ? 'interval' OR query ? 'duration'
        OR operator IN ('timestamp', 'interval', 'duration')
    ) THEN
        RETURN _temporal_literal(query);
    END IF;

    -- handle spatial operators
    IF operator ^@ 's_' or operator = 'intersects' THEN
        RETURN _spatial_operators(query, prefixes);
    END IF;

    -- handle array operators
    IF operator ^@ 'a_' THEN
        RETURN _array_operators(query, prefixes);
    END IF;

    -- recursively resolve operator arguments
    IF query ? 'args' THEN

        -- make sure the arguments are an array
        IF jsonb_typeof(arguments) <> 'array' THEN
            arguments := jsonb_build_array(arguments);
        END IF;

        -- flatten the IN arguments
        IF operator = 'in' THEN
            arguments := jsonb_build_array(arguments->0) || (arguments->1);
        END IF;

        -- normalize the BETWEEN arguments
        IF operator = 'between' THEN
            arguments = jsonb_build_array(
                arguments->0,
                arguments->1,
                arguments->2
            );
        END IF;

        SELECT jsonb_agg(cql2_to_sql_predicate(item, prefixes))
            INTO arguments
        FROM jsonb_array_elements(arguments) AS item;

    END IF;

    -- handle CQL2 operators
    IF query ? 'op' THEN

        IF operator in ('and', 'or') THEN
            template := CASE operator
                WHEN 'and' THEN '(%s AND %s)'
                WHEN 'or' THEN '(%s OR %s)'
            END;
            RETURN format(template, VARIADIC (pgstac.to_text_array(arguments)));
        END IF;

        IF operator = 'in' THEN
            RETURN format(
                '%s IN (%s)',
                arguments->>0,
                array_to_string((pgstac.to_text_array(arguments))[2:], ',')
            );
        END IF;

        -- pair-search specific numeric difference operator
        IF operator = 'n_diff' THEN
            operator := '-';
        END IF;

        -- look up operator template from cql2_ops table
        SELECT cql2_ops.template INTO template FROM cql2_ops WHERE  cql2_ops.op ilike operator;
        IF FOUND THEN
            RETURN format(template, VARIADIC (pgstac.to_text_array(arguments)));
        ELSE
            RAISE EXCEPTION 'Operator % Not Supported.', op;
        END IF;
    END IF;

    -- handle properties and simple literals
    IF query ? 'property' THEN
        property := query->>'property';
        queryable := _translate_queryable(property, prefixes);
        IF queryable.wrapper IS NOT NULL THEN
            RETURN format('%s(%s)', queryable.wrapper, queryable.path);
        END IF;
        RETURN queryable.path;
    END IF;

    IF jsonb_typeof(query) = 'number' THEN
        IF _is_int(query->>0) THEN
            RETURN format('%L::int', query->>0);
        END IF;
        RETURN format('%L::float', query->>0);
    END IF;

    RETURN quote_literal(pgstac.to_text(query));

END;
$$ LANGUAGE PLPGSQL; -- STABLE;


-- handle array operators
CREATE OR REPLACE FUNCTION _array_operators(
    query jsonb,
    prefixes text[] DEFAULT NULL
) RETURNS text AS $$
DECLARE
    operator text := lower(query->>'op');
    arguments jsonb := query->'args';
    sql_operator text;
    template text;
BEGIN
    sql_operator := CASE operator
        WHEN 'a_equals'         THEN '='
        WHEN 'a_contains'       THEN '@>'
        WHEN 'a_contained_by'   THEN '<@'
        WHEN 'a_overlaps'       THEN '&&'
    END;
    IF sql_operator IS NOT NULL THEN
        RETURN format(
            '%s %s %s',
            _array_argument(arguments->0, prefixes),
            sql_operator,
            _array_argument(arguments->1, prefixes)
        );
    END IF;

    RAISE EXCEPTION 'Invalid array operator! %', operator;
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


CREATE OR REPLACE FUNCTION _array_argument(
    query jsonb,
    prefixes text[] DEFAULT NULL
) RETURNS text AS $$
BEGIN
    IF jsonb_typeof(query) = 'array' THEN
        RETURN quote_literal(to_text_array(query));
    END IF;
    RETURN cql2_to_sql_predicate(query, prefixes);
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


-- handle spatial operators
CREATE OR REPLACE FUNCTION _spatial_operators(
    IN query jsonb,
    prefixes text[] DEFAULT NULL
) RETURNS text AS $$
DECLARE
    operator text := lower(query->>'op');
    arguments jsonb := query->'args';
    sql_operator text;
    template text;
BEGIN

    IF operator = 's_raoverlap' THEN
        sql_operator := CASE arguments->>2
            WHEN 'first'    THEN 'st_relative_overlap_first'
            WHEN 'second'   THEN 'st_relative_overlap_second'
            WHEN 'max'      THEN 'st_relative_overlap_max'
            WHEN 'min'      THEN 'st_relative_overlap_min'
        END;
        IF sql_operator IS NULL THEN
            RAISE EXCEPTION 'Invalid s_raoverlap divisor selection! %s', arguments->>2;
        END IF;
    ELSE
        sql_operator := CASE operator
            WHEN 'intersects'   THEN 'st_intersects'
            WHEN 's_contains'   THEN 'st_contains'
            WHEN 's_crosses'    THEN 'st_crosses'
            WHEN 's_disjoint'   THEN 'st_disjoint'
            WHEN 's_equals'     THEN 'st_equals'
            WHEN 's_intersects' THEN 'st_intersects'
            WHEN 's_overlaps'   THEN 'st_overlaps'
            WHEN 's_touches'    THEN 'st_touches'
            WHEN 's_within'     THEN 'st_within'
        END;
    END IF;

    IF sql_operator IS NULL THEN
        RAISE EXCEPTION 'Invalid spatial operator! %', operator;
    END IF;

    RETURN format(
        '%s(%s, %s)',
        sql_operator,
        _geom_argument(arguments->0, prefixes),
        _geom_argument(arguments->1, prefixes)
    );
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


CREATE OR REPLACE FUNCTION _geom_argument(
    query jsonb,
    prefixes text[] DEFAULT NULL
) RETURNS text AS $$
DECLARE
    operator text;
    arguments jsonb;
BEGIN
    IF query ? 'type' AND query ? 'coordinates' THEN
        RETURN format('%L::geometry', st_geomfromgeojson(query)::text);
    ELSIF query ? 'bbox' THEN
        RETURN format('%L::geometry', bbox_geom(query->'bbox')::text);
    ELSIF jsonb_typeof(query) = 'array' THEN
        RETURN format('%L::geometry', bbox_geom(query)::text);
    END IF;

    IF query ? 'op' AND query ? 'args' THEN
        operator := query->>'op';
        arguments := query->'args';
        IF jsonb_typeof(arguments) = 'array' THEN
            IF operator = 'bbox' AND jsonb_array_length(arguments) = 4 THEN
                RETURN _geom_argument(jsonb_build_object(operator, arguments));
            END IF;
        END IF;
    END IF;

    RETURN cql2_to_sql_predicate(query, prefixes);
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


-- handle temporal operators
CREATE OR REPLACE FUNCTION _temporal_operators(
    IN query jsonb,
    prefixes text[] DEFAULT NULL
) RETURNS text AS $$
DECLARE
    operator text := lower(query->>'op');
    arguments jsonb := query->'args';
    template text;
    result text;
    range record;
    _left text;
    _right text;
BEGIN
    template := CASE operator
        WHEN 't_before'       THEN '(lh < rl)'
        WHEN 't_after'        THEN '(ll > rh)'
        WHEN 't_meets'        THEN '(lh = rl)'
        WHEN 't_metby'        THEN '(ll = rh)'
        WHEN 't_overlaps'     THEN '(ll < rl AND rl < lh < rh)'
        WHEN 't_overlappedby' THEN '(rl < ll < rh AND lh > rh)'
        WHEN 't_starts'       THEN '(ll = rl AND lh < rh)'
        WHEN 't_startedby'    THEN '(ll = rl AND lh > rh)'
        WHEN 't_during'       THEN '(ll > rl AND lh < rh)'
        WHEN 't_contains'     THEN '(ll < rl AND lh > rh)'
        WHEN 't_finishes'     THEN '(ll > rl AND lh = rh)'
        WHEN 't_finishedby'   THEN '(ll < rl AND lh = rh)'
        WHEN 't_equals'       THEN '(ll = rl AND lh = rh)'
        WHEN 't_disjoint'     THEN '(NOT (ll <= rh AND lh >= rl))'
        WHEN 't_intersects'   THEN '(ll <= rh AND lh >= rl)'
        WHEN 'anyinteracts'   THEN '(ll <= rh AND lh >= rl)'
    END;
    IF template IS NOT NULL THEN
        range := _temp_range_argument(arguments->0, prefixes);
        result := regexp_replace(template, '\mll\M', range._start);
        result := regexp_replace(result, '\mlh\M', range._end);
        range := _temp_range_argument(arguments->1, prefixes);
        result := regexp_replace(result, '\mrl\M', range._start);
        result := regexp_replace(result, '\mrh\M', range._end);
        RETURN result;
    END IF;

    -- pair-search specific operator
    template := CASE operator
        WHEN 't_start'  THEN 'll'
        WHEN 't_end'    THEN 'lh'
    END;
    IF template IS NOT NULL THEN
        range := _temp_range_argument(arguments->0, prefixes);
        result := regexp_replace(template, '\mll\M', range._start);
        result := regexp_replace(result, '\mlh\M', range._end);
        RETURN result;
    END IF;

    IF operator = 't_diff' THEN
        RETURN cql2_to_sql_predicate(
            jsonb_build_object('op', '-', 'args', arguments),
            prefixes
        );
    END IF;

    RAISE EXCEPTION 'Invalid temporal operator! %', operator;
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


CREATE OR REPLACE FUNCTION _temp_range_argument(
    IN query jsonb,
    prefixes text[] DEFAULT NULL,
    OUT _start text,
    OUT _end text
) AS $$
DECLARE
    temp record;
    property text;
BEGIN
    IF query ? 'property' THEN
        property := query->>'property';
        temp := _split_prefix(property, prefixes);
        IF temp.name = 'datetime' THEN
            _start := cql2_to_sql_predicate(
                jsonb_build_object('property', temp.prefix || 'datetime'),
                prefixes
            );
            _end := cql2_to_sql_predicate(
                jsonb_build_object('property', temp.prefix || 'end_datetime'),
                prefixes
            );
        ELSE
            _start := cql2_to_sql_predicate(query, prefixes);
            _end := _start;
        END IF;
    ELSE
        temp = _range_temporal_literal(query);
        _start := temp._start;
        _end := temp._end;
    END IF;
    RETURN;
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


-- parse temporal literals as temporal ranges
CREATE OR REPLACE FUNCTION _range_temporal_literal(
    IN query jsonb,
    OUT _start text,
    OUT _end text
) AS $$
DECLARE
    temp record;
    operator text;
    arguments jsonb;
BEGIN

    IF query ? 'timestamp' THEN
        _start := format('%L::timestamptz', (query->'timestamp'->>0)::timestamptz);
        _end := _start;
        RETURN;
    END IF;

    IF query ? 'interval' AND jsonb_typeof(query->'interval') = 'array' THEN
        IF jsonb_array_length(query->'interval') = 2 THEN
            _start := format('%L::timestamptz', (query->'interval'->>0)::timestamptz);
            _end := format('%L::timestamptz', (query->'interval'->>1)::timestamptz);
            RETURN;
        END IF;
    END IF;

    IF query ? 'op' AND query ? 'args' THEN
        operator := query->>'op';
        arguments := query->'args';
        IF jsonb_typeof(arguments) = 'array' THEN
            IF operator = 'timestamp' AND jsonb_array_length(arguments) = 1 THEN
                temp = _range_temporal_literal(jsonb_build_object(operator, arguments->0));
                _start := temp._start;
                _end := temp._end;
                RETURN;
            END IF;
            IF operator = 'interval' AND jsonb_array_length(arguments) = 2 THEN
                temp = _range_temporal_literal(jsonb_build_object(operator, arguments));
                _start := temp._start;
                _end := temp._end;
                RETURN;
            END IF;
        END IF;
    END IF;

    RAISE EXCEPTION 'Invalid temporal range literal! %', query;
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


-- parse temporal literals
CREATE OR REPLACE FUNCTION _temporal_literal(
    query jsonb
) RETURNS text AS $$
DECLARE
    operator text;
    arguments jsonb;
BEGIN

    IF query ? 'timestamp' THEN
        RETURN format('%L::timestamptz', (query->'timestamp'->>0)::timestamptz);
    END IF;

    IF query ? 'interval' AND jsonb_typeof(query->'interval') = 'array' THEN
        IF jsonb_array_length(query->'interval') = 1 THEN
            RETURN format('%L::interval', (query->'interval'->>0)::interval);
        END IF;

        IF jsonb_array_length(query->'interval') = 2 THEN
            RETURN format('%L::interval', (
               (query->'interval'->>1)::timestamptz - (query->'interval'->>0)::timestamptz)
            );
        END IF;
    END IF;

    -- pair-search specific type
    IF query ? 'duration' THEN
        RETURN _temporal_literal(jsonb_build_object('interval', query->'duration'));
    END IF;

    IF query ? 'op' AND query ? 'args' THEN
        operator := query->>'op';
        arguments := query->'args';
        IF jsonb_typeof(arguments) = 'array' THEN
            IF operator = 'timestamp' AND jsonb_array_length(arguments) = 1 THEN
                RETURN _temporal_literal(jsonb_build_object(operator, arguments->0));
            END IF;
            -- pair-search specific type
            IF operator = 'duration' THEN
                operator := 'interval';
            END IF;
            IF operator = 'interval' AND jsonb_array_length(arguments) IN (1,2) THEN
                RETURN _temporal_literal(jsonb_build_object(operator, arguments));
            END IF;
        END IF;
    END IF;

    RAISE EXCEPTION 'Invalid temporal literal! %', query;
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


-- Enforce that all properties are defined as queryables
CREATE OR REPLACE PROCEDURE _enforce_queryables(
    IN filter jsonb,
    IN prefixes text[] DEFAULT NULL
) AS $$
DECLARE
    property text;
BEGIN
    FOR property IN
        SELECT DISTINCT item->>0
        FROM jsonb_path_query(filter, 'strict $.**.property') AS item
    LOOP
        IF NOT _is_queryable(property, prefixes) THEN
            RAISE EXCEPTION 'Property % is not queryable!', property;
        END IF;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL; -- STABLE;


-- Conversion of properties to pgstac specific variables
CREATE OR REPLACE FUNCTION _translate_queryable(
    IN property text,
    IN prefixes text[] DEFAULT NULL,
    OUT path text,
    OUT expression text,
    OUT wrapper text,
    OUT nulled_wrapper text
) AS $$
DECLARE
    temp record;
    prefix text := '';
    property_sans_prefix text;
BEGIN
    temp := _split_prefix(property, prefixes);
    prefix := temp.prefix;
    property_sans_prefix := temp.name;

    temp := pgstac.queryable(property_sans_prefix);
    path := prefix || temp.path;
    wrapper := _resolve_namespace(temp.wrapper);
    expression := path;

    IF wrapper IS NOT NULL THEN
        expression := format('%s(%s)', wrapper, expression);
    END IF;

    nulled_wrapper := _resolve_namespace(temp.nulled_wrapper);
END;
$$ LANGUAGE PLPGSQL; --STABLE STRICT;


-- resolve namespace for the type converting wrapper
CREATE OR REPLACE FUNCTION _resolve_namespace(
    name text,
    arguments text DEFAULT 'jsonb'
) RETURNS text AS $$
DECLARE
    namespace text;
BEGIN
    IF name IS NOT NULL THEN
        SELECT nspname INTO namespace
          FROM pg_proc, pg_namespace
          WHERE pg_namespace.oid = pg_proc.pronamespace
            AND (pg_proc.oid)::regprocedure::text = format('%s(%s)', name, arguments);
        IF FOUND AND namespace IS NOT NULL AND namespace <> '' THEN
            RETURN format('%I.%I', namespace, name);
        END IF;
    END IF;
    RETURN name;
END;
$$ LANGUAGE PLPGSQL STRICT; --STABLE;


-- split base prefixed property name
CREATE OR REPLACE FUNCTION _split_prefix(
    IN property text,
    IN prefixes text[] DEFAULT NULL,
    OUT prefix text,
    OUT name text
) AS $$
DECLARE
BEGIN
    IF prefixes IS NULL THEN
        prefix := '';
        name := property;
        RETURN;
    END IF;
    -- detect test prefixed property
    FOREACH prefix IN ARRAY prefixes LOOP
        IF property ^@ prefix THEN
            name := substring(property, char_length(prefix) + 1);
            RETURN;
        END IF;
    END LOOP;
    RAISE EXCEPTION 'Invalid property name! %', property;
END;
$$ LANGUAGE PLPGSQL; -- IMMUTABLE STRICT PARALLEL SAFE;

-- test if property is a queryable
CREATE OR REPLACE FUNCTION _is_queryable(
    IN property text,
    IN prefixes text[] DEFAULT NULL
) RETURNS bool AS $$
DECLARE
    temp record;
BEGIN
    temp := _split_prefix(property, prefixes);
    RETURN _test_queryable(temp.name);
END;
$$ LANGUAGE PLPGSQL; --STABLE STRICT;


-- low-level sans prefix is-queryable test
CREATE OR REPLACE FUNCTION _test_queryable(
    IN property text
) RETURNS bool AS $$
DECLARE
    match int;
    prefix text := 'properties.';
BEGIN
    IF property IN ('id', 'geometry', 'datetime', 'end_datetime', 'collection') THEN
        RETURN TRUE;
    END IF;

    IF property ^@ prefix THEN
        property := substring(property, char_length(prefix) + 1);
    END IF;

    SELECT 1 INTO match FROM queryables
        WHERE name = property OR name = prefix || property ;
    RETURN match IS NOT NULL;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;


-- test if string is an integer
CREATE OR REPLACE FUNCTION _is_int(value text) RETURNS bool AS $$
BEGIN
    RETURN value ~ '^[+-]?[0-9]+$';
END;
$$ LANGUAGE PLPGSQL IMMUTABLE STRICT PARALLEL SAFE;


-- quantitative area overlap spatial operators
CREATE OR REPLACE FUNCTION st_relative_overlap_first(
    geom_first geometry,
    geom_second geometry
) RETURNS double precision AS $$
DECLARE
    area_dividend double precision;
    area_divisor double precision;
BEGIN
    area_dividend := st_area(st_intersection(geom_first, geom_second));
    IF area_dividend > 0 THEN
        area_divisor := st_area(geom_first);
        RETURN area_dividend / area_divisor;
    END IF;
    RETURN area_dividend;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION st_relative_overlap_second(
    geom_first geometry,
    geom_second geometry
) RETURNS double precision AS $$
DECLARE
    area_dividend double precision;
    area_divisor double precision;
BEGIN
    area_dividend := st_area(st_intersection(geom_first, geom_second));
    IF area_dividend > 0 THEN
        area_divisor := st_area(geom_second);
        RETURN area_dividend / area_divisor;
    END IF;
    RETURN area_dividend;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION st_relative_overlap_max(
    geom_first geometry,
    geom_second geometry
) RETURNS double precision AS $$
DECLARE
    area_dividend double precision;
    area_divisor double precision;
BEGIN
    area_dividend := st_area(st_intersection(geom_first, geom_second));
    IF area_dividend > 0 THEN
        area_divisor := GREATEST(st_area(geom_first), st_area(geom_second));
        RETURN area_dividend / area_divisor;
    END IF;
    RETURN area_dividend;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION st_relative_overlap_min(
    geom_first geometry,
    geom_second geometry
) RETURNS double precision AS $$
DECLARE
    area_dividend double precision;
    area_divisor double precision;
BEGIN
    area_dividend := st_area(st_intersection(geom_first, geom_second));
    IF area_dividend > 0 THEN
        area_divisor := LEAST(st_area(geom_first), st_area(geom_second));
        RETURN area_dividend / area_divisor;
    END IF;
    RETURN area_dividend;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE STRICT PARALLEL SAFE;
