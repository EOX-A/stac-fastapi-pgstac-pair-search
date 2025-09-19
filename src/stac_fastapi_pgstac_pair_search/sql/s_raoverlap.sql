CREATE OR REPLACE FUNCTION S_RAOVERLAP(a TEXT, b TEXT, divisor TEXT)
RETURNS DOUBLE PRECISION AS $$
DECLARE
  geom_a GEOMETRY;
  geom_b GEOMETRY;
  area_a DOUBLE PRECISION;
  area_b DOUBLE PRECISION;
  area_intersection DOUBLE PRECISION;
  divisor_area DOUBLE PRECISION;
BEGIN
  -- 1. Handle NULL or invalid GeoJSON Feature inputs
  IF a IS NULL OR b IS NULL THEN
    RETURN NULL;
  END IF;

  -- 2. Extract and convert the GeoJSON geometry to a PostGIS geometry
  geom_a := ST_GeomFromGeoJSON(a);
  geom_b := ST_GeomFromGeoJSON(b);

  -- 3. Optimization: If geometries do not intersect, the overlap is zero
  IF NOT ST_Intersects(geom_a, geom_b) THEN
    RETURN 0.0;
  END IF;

  -- 4. Calculate the areas
  area_a := ST_Area(geom_a);
  area_b := ST_Area(geom_b);
  area_intersection := ST_Area(ST_Intersection(geom_a, geom_b));

  -- 5. Determine the divisor area based on the input string
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

  -- 6. Handle the zero-area divisor case by returning NaN
  IF divisor_area = 0.0 THEN
    RETURN 'NaN'::double precision;
  END IF;

  -- 7. Return the final ratio
  RETURN area_intersection / divisor_area;
END;
$$ LANGUAGE plpgsql;
