
-- 1. Core Logic Function (GEOMETRY inputs)
-- This is the main function that performs the calculation.
---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION S_RAOVERLAP(a GEOMETRY, b GEOMETRY, divisor TEXT)
RETURNS DOUBLE PRECISION AS $$
DECLARE
  area_a DOUBLE PRECISION;
  area_b DOUBLE PRECISION;
  area_intersection DOUBLE PRECISION;
  divisor_area DOUBLE PRECISION;
BEGIN
  IF a IS NULL OR b IS NULL THEN
    RETURN NULL;
  END IF;

  IF NOT ST_Intersects(a, b) THEN
    RETURN 0.0;
  END IF;

  area_a := ST_Area(a);
  area_b := ST_Area(b);
  area_intersection := ST_Area(ST_Intersection(a, b));

  CASE lower(divisor)
    WHEN 'first' THEN
      divisor_area := area_a;
    WHEN 'second' THEN
      divisor_area := area_b;
    WHEN 'min' THEN
      divisor_area := LEAST(area_a, area_b);
    WHEN 'max' THEN
      divisor_area := GREATEST(area_a, area_b);
    ELSE
      RAISE EXCEPTION 'Invalid divisor: %. Must be one of first, second, min, max', divisor;
  END CASE;

  IF divisor_area = 0.0 THEN
    RETURN 'NaN'::double precision;
  END IF;

  RETURN area_intersection / divisor_area;
END;
$$ LANGUAGE plpgsql;


-- 2. Wrapper for GeoJSON Feature (JSONB) inputs
---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION S_RAOVERLAP(a JSONB, b JSONB, divisor TEXT)
RETURNS DOUBLE PRECISION AS $$
BEGIN
  RETURN S_RAOVERLAP(
    ST_GeomFromGeoJSON(a),
    ST_GeomFromGeoJSON(b),
    divisor
  );
END;
$$ LANGUAGE plpgsql;


-- 3. Wrapper for WKT (TEXT) inputs
---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION S_RAOVERLAP(a TEXT, b TEXT, divisor TEXT)
RETURNS DOUBLE PRECISION AS $$
BEGIN
  RETURN S_RAOVERLAP(
    ST_GeomFromText(a, 4326),
    ST_GeomFromText(b, 4326),
    divisor
  );
END;
$$ LANGUAGE plpgsql;


-- 4. Wrappers for Mixed GeoJSON and WKT inputs
---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION S_RAOVERLAP(a TEXT, b JSONB, divisor TEXT)
RETURNS DOUBLE PRECISION AS $$
BEGIN
  RETURN S_RAOVERLAP(
    ST_GeomFromText(a, 4326),
    ST_GeomFromGeoJSON(b),
    divisor
  );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION S_RAOVERLAP(a JSONB, b TEXT, divisor TEXT)
RETURNS DOUBLE PRECISION AS $$
BEGIN
  RETURN S_RAOVERLAP(
    ST_GeomFromGeoJSON(a),
    ST_GeomFromText(b, 4326),
    divisor
  );
END;
$$ LANGUAGE plpgsql;


-- 5. NEW: Wrappers for Mixed GEOMETRY and WKT inputs
-- These resolve the "function is not unique" ambiguity.
---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION S_RAOVERLAP(a GEOMETRY, b TEXT, divisor TEXT)
RETURNS DOUBLE PRECISION AS $$
BEGIN
  -- Convert the TEXT input to GEOMETRY and call the core function
  RETURN S_RAOVERLAP(a, ST_GeomFromText(b, 4326), divisor);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION S_RAOVERLAP(a TEXT, b GEOMETRY, divisor TEXT)
RETURNS DOUBLE PRECISION AS $$
BEGIN
  -- Convert the TEXT input to GEOMETRY and call the core function
  RETURN S_RAOVERLAP(ST_GeomFromText(a, 4326), b, divisor);
END;
$$ LANGUAGE plpgsql;
