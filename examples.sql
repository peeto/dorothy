-- Query examples

/*
-- Rebuild
CALL destroy_previd();
CALL destroy_cache();
CALL repair_previd(1000, @results); -- repeat
SELECT @results;
CALL repair_cache(1000, @results); -- repeat
SELECT @results;
CALL optimise_cache(1000, @results); -- repeat
SELECT @results;
CALL repair_cache(1000, @results); -- repeat
SELECT @results;
*/

-- Most popular locations
SELECT * FROM 
  (
    SELECT X(lc.location) AS lat, Y(lc.location) AS `long`,
      COUNT(l.id) AS visits,
      CONCAT('https://www.google.com/maps/place/',X(lc.location),',',Y(lc.location)) AS url
    FROM locations_cache AS lc 
    JOIN locations AS l ON l.cacheid=lc.id 
    GROUP BY lc.location
) AS popular 
ORDER BY visits DESC
LIMIT 10;

-- Longest journey's
SELECT
  l.id, l.user, l.device,
  X(l.location) AS lat, Y(l.location) AS `long`,
  CONCAT('https://www.google.com/maps/place/',X(l.location),',',Y(l.location)) AS url,
  l.altitude, l.time, l.prevdist AS distance,
  lp.time as prevtime, l.time - lp.time AS totaltime,
  l.prevdist/(l.time - lp.time) AS speed_ms,
  (l.prevdist/(l.time - lp.time))*3.6 AS speed_kmh
FROM locations AS l
LEFT JOIN locations lp ON lp.id=l.previd
GROUP BY l.user, l.device, l.id
ORDER BY l.prevdist DESC
LIMIT 5;

-- bad/flight data
SELECT
  l.id, l.user, l.device,
  X(l.location) AS lat, Y(l.location) AS `long`,
  CONCAT('https://www.google.com/maps/place/',X(l.location),',',Y(l.location)) AS url,
  l.altitude, l.time, l.prevdist AS distance,
  CONCAT('https://www.google.com/maps/place/',X(lp.location),',',Y(lp.location)) AS prevurl,
  lp.time as prevtime, l.time - lp.time AS totaltime,
  l.prevdist/(l.time - lp.time) AS speed_ms,
  (l.prevdist/(l.time - lp.time))*3.6 AS speed_kmh
FROM locations AS l
LEFT JOIN locations lp ON lp.id=l.previd
WHERE (l.prevdist/(l.time - lp.time))*3.6 > 200
GROUP BY l.user, l.device, l.id
ORDER BY (l.prevdist/(l.time - lp.time)) DESC;

-- Monthly distance report
SELECT 
  l.user, l.device, 
  YEAR(l.time) AS `year`, MONTHNAME(l.time) AS `month`,
  SUM(prevdist) AS totaldist, SUM(prevdist)/1000 AS totalkm
FROM locations AS l
GROUP BY l.user, l.device, YEAR(l.time), MONTH(l.time)
ORDER BY l.user, l.device, YEAR(l.time) DESC, MONTH(l.time) DESC;

-- Database info
SHOW PROCEDURE STATUS;
SHOW TABLES;
