-- Dorothy DDL

CREATE DATABASE IF NOT EXISTS Dorothy;
USE Dorothy;

CREATE TABLE IF NOT EXISTS `locations` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user` varchar(255) DEFAULT NULL,
  `device` varchar(255) DEFAULT NULL,
  `location` point DEFAULT NULL,
  `altitude` float DEFAULT NULL,
  `time` datetime DEFAULT NULL,
  `previd` int(11) DEFAULT NULL,
  `prevdist` double NOT NULL DEFAULT 0,
  `cacheid` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`),
  KEY `idx_user` (`user`),
  KEY `idx_device` (`device`),
  KEY `idx_time` (`time`),
  KEY `idx_location` (`location`(25)),
  KEY `idx_previd` (`previd`),
  KEY `idx_cacheid` (`cacheid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `locations_cache` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `location` point DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_location` (`location`(25))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- destroy_previd
DROP PROCEDURE IF EXISTS destroy_previd;
delimiter //
CREATE PROCEDURE destroy_previd ()
BEGIN
   UPDATE locations SET previd=NULL, prevdist=0;
END//
delimiter ;

-- repair_previd
DROP PROCEDURE IF EXISTS repair_previd;
delimiter //
CREATE PROCEDURE repair_previd (IN workload INT, OUT workleft INT)
BEGIN
  START TRANSACTION;
    CREATE TEMPORARY TABLE IF NOT EXISTS 
      idcache AS (
        SELECT 
            l.id, lp.id AS previd,
            IFNULL(ROUND(GLength(LineStringFromWKB(
              LineString(l.location, lp.location)
              ))*110400), 0) AS prevdist
          FROM locations AS l
          JOIN locations AS lp
            ON lp.id=(
              SELECT lc.id FROM locations AS lc
                WHERE
                  lc.user = l.user
                  AND lc.device = l.device
                  AND lc.time < l.time
                ORDER BY lc.time DESC 
                LIMIT 1
            )
          WHERE l.previd IS NULL 
          AND lp.time < l.time
          ORDER BY l.id DESC 
          LIMIT workload
      );
    UPDATE locations 
      JOIN idcache ON idcache.id = locations.id
      SET locations.previd = idcache.previd,
        locations.prevdist = idcache.prevdist
      WHERE
        locations.previd IS NULL AND 
        idcache.id IS NOT NULL;
    DROP TEMPORARY TABLE IF EXISTS idcache;
    SELECT COUNT(1) INTO workleft FROM locations WHERE previd IS NULL;
  COMMIT;
END//
delimiter ;

-- destroy_cache
DROP PROCEDURE IF EXISTS destroy_cache;
delimiter //
CREATE PROCEDURE destroy_cache ()
BEGIN
  TRUNCATE locations_cache;
  UPDATE locations SET cacheid=NULL;
END//
delimiter ;

-- repair_cache
DROP PROCEDURE IF EXISTS repair_cache;
delimiter //
CREATE PROCEDURE repair_cache (IN workload INT, OUT workleft INT)
BEGIN
  DECLARE minrange INT;
  DECLARE minpoints INT;
  DECLARE done INT DEFAULT FALSE;
  DECLARE insert_id INT; 
  DECLARE insert_location POINT; 
  DECLARE cache_id INT; 
  DECLARE cacheCursor CURSOR 
    FOR 
      SELECT id, location, cacheid FROM idcache GROUP BY id;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
  START TRANSACTION;
    SET minrange = 100;
    SET minpoints = 10;
    -- get locations that need and have a cache and also locations that need a new cache
    CREATE TEMPORARY TABLE IF NOT EXISTS 
      idcache AS (
        SELECT 
            l.id, l.location, (SELECT id FROM locations_cache AS lc WHERE IFNULL(ROUND(GLength(LineStringFromWKB(LineString(l.location, lc.location)))*110400), minrange) < minrange ORDER BY id LIMIT 1) AS cacheid
          FROM locations AS l
          WHERE 
            l.cacheid IS NULL 
          AND 
            (
              (SELECT id FROM locations_cache AS lc WHERE IFNULL(ROUND(GLength(LineStringFromWKB(LineString(l.location, lc.location)))*110400), minrange) < minrange ORDER BY id LIMIT 1) IS NOT NULL
            OR
              (SELECT COUNT(1) FROM locations AS ln WHERE IFNULL(ROUND(GLength(LineStringFromWKB(LineString(l.location, ln.location)))*110400), minrange) < minrange) >= minpoints
            )
          GROUP BY l.id
          ORDER BY l.id DESC 
          LIMIT workload
      );
    -- updates locations that need and have an existing cache
    UPDATE locations 
      JOIN idcache ON idcache.id = locations.id
      SET locations.cacheid = idcache.cacheid
      WHERE
        locations.cacheid IS NULL
        AND idcache.id IS NOT NULL
        AND idcache.cacheid IS NOT NULL;
    -- remove completed work
    DELETE FROM idcache 
      WHERE cacheid IS NOT NULL;
    -- create new cached locations, may create duplicates that will be dealt with next
    OPEN cacheCursor;
    read_loop: LOOP
      FETCH cacheCursor INTO insert_id, insert_location, cache_id;
      IF done THEN
        LEAVE read_loop;
      END IF;
      IF cache_id IS NULL THEN
        SELECT lc.id INTO cache_id FROM locations_cache AS lc WHERE IFNULL(ROUND(GLength(LineStringFromWKB(LineString(insert_location, lc.location)))*110400), minrange) < minrange;
        IF cache_id IS NULL THEN
  	  INSERT INTO locations_cache (location) VALUES (insert_location);
          UPDATE locations SET cacheid = LAST_INSERT_ID() WHERE IFNULL(ROUND(GLength(LineStringFromWKB(LineString(insert_location, location)))*110400), minrange) < minrange;
        ELSE
          UPDATE locations SET cacheid = cache_id WHERE IFNULL(ROUND(GLength(LineStringFromWKB(LineString(insert_location, location)))*110400), minrange) < minrange;
        END IF;
      END IF;
    END LOOP;   
    DROP TEMPORARY TABLE IF EXISTS idcache;
    SELECT COUNT(1) 
      INTO workleft 
      FROM locations 
      WHERE cacheid IS NULL;
  COMMIT;
END//
delimiter ;

-- optimise_cache
DROP PROCEDURE IF EXISTS optimise_cache;
delimiter //
CREATE PROCEDURE optimise_cache (IN workload INT, OUT workleft INT)
BEGIN
  DECLARE minrange INT;
  DECLARE minpoints INT;
  START TRANSACTION;
    SET minrange = 100;
    SET minpoints = 10;
    -- find any duplicates in the cache that have less duplicates than a nearby duplicate in the cache
    DROP TEMPORARY TABLE IF EXISTS idcache;
    CREATE TEMPORARY TABLE IF NOT EXISTS 
      idcache AS (
        SELECT id, location, cacheid FROM (
          SELECT lc.id, lc.location, lc.id AS cacheid
            FROM locations_cache AS lc
            WHERE 
                  (SELECT COUNT(1) FROM locations AS ics WHERE ics.cacheid = lc.id)
                < 
                  minpoints
              AND 
                  (SELECT COUNT(1) FROM locations AS ics WHERE ics.cacheid = lc.id) 
                < 
                  (SELECT COUNT(1) AS total
                    FROM locations AS ics 
                    WHERE ics.cacheid IS NOT NULL 
                      AND ics.cacheid <> lc.id
                      AND IFNULL(ROUND(GLength(LineStringFromWKB(LineString(lc.location, ics.location)))*110400), minrange) < minrange
                    GROUP BY ics.cacheid
                    ORDER BY COUNT(1) DESC
                    LIMIT 1
                  )
            GROUP BY lc.location
            ORDER BY lc.id
            LIMIT workload
        ) AS duplicates
      );
    -- free existing locations from cache to be killed
    UPDATE locations SET cacheid=NULL WHERE cacheid IN (
        SELECT id FROM idcache GROUP BY id
      );
    -- delete redundant location caches
    DELETE FROM locations_cache WHERE id IN (
        SELECT id FROM idcache GROUP BY id
      );
    -- delete unused from cache
    DELETE FROM locations_cache WHERE id NOT IN (
        SELECT lc.id 
          FROM locations AS lc
          WHERE lc.cacheid IS NOT NULL
          GROUP BY lc.cacheid
      );
    DROP TEMPORARY TABLE IF EXISTS idcache;
    SELECT COUNT(1) 
      INTO workleft 
      FROM locations 
      WHERE cacheid IS NULL;
  COMMIT;
END//
delimiter ;

-- insert_location
DROP PROCEDURE IF EXISTS insert_current_location;
DROP PROCEDURE IF EXISTS insert_location;
delimiter //
CREATE PROCEDURE insert_location (OUT out_id INT, IN in_user VARCHAR(255), IN in_device VARCHAR(255), IN in_lat DOUBLE, IN in_long DOUBLE, IN in_alt DOUBLE, IN in_time DATETIME)
BEGIN
  DECLARE cache_id INT;
  START TRANSACTION;
    SELECT lc.id INTO cache_id FROM locations_cache AS lc WHERE IFNULL(ROUND(GLength(LineStringFromWKB(LineString(lc.location, GeomFromText(CONCAT('POINT(', in_lat, ' ', in_long, ')')))))*110400), 100) < 100;
    IF cache_id IS NULL THEN
      INSERT INTO locations_cache (location) VALUES (GeomFromText(CONCAT('POINT(', in_lat, ' ', in_long, ')')));
      SET cache_id = LAST_INSERT_ID();
    END IF;
    INSERT INTO `locations` SET 
      `time` = in_time,
      `location`= GeomFromText(CONCAT('POINT(', in_lat, ' ', in_long, ')')),
      `user`= in_user,
      `device`= in_device,
      `altitude`= in_alt,
      previd=(SELECT id FROM `locations` AS lc WHERE 
        lc.`user`= in_user AND
        lc.`device`= in_device AND
        lc.time < in_time
        ORDER BY lc.time DESC LIMIT 1),
      prevdist=IFNULL(ROUND(GLength(LineStringFromWKB(LineString(
        GeomFromText(CONCAT('POINT(', in_lat, ' ', in_long, ')')),
        (SELECT location FROM `locations` AS lc WHERE
          lc.`user`= in_user AND
          lc.`device`= in_device AND
          lc.time < in_time ORDER BY lc.time DESC LIMIT 1)
      )))*110400), 0),
      cacheid=cache_id;
    SET out_id = LAST_INSERT_ID();
  COMMIT;
END//
delimiter ;

-- insert_current_location
DROP PROCEDURE IF EXISTS insert_current_location;
delimiter //
CREATE PROCEDURE insert_current_location (OUT out_id INT, IN in_user VARCHAR(255), IN in_device VARCHAR(255), IN in_lat DOUBLE, IN in_long DOUBLE, IN in_alt DOUBLE)
BEGIN
  CALL insert_location(out_id, in_user, in_device, in_lat, in_long, in_alt, NOW());
END//
delimiter ;

