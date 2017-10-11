DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv;

-- Question 0
CREATE VIEW q0(era) 
AS
  SELECT MAX(era) FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM master WHERE weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear FROM master WHERE namefirst LIKE '% %';
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear,
         AVG(height) AS avgheight,
         COUNT(*) AS count
  FROM master GROUP BY birthyear
  ORDER BY birthyear ASC
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear,
         AVG(height) AS avgheight,
         COUNT(*) AS count
  FROM master 
  GROUP BY birthyear
  HAVING AVG(height) > 70
  ORDER BY birthyear ASC
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT m.namefirst, m.namelast, h.playerid, h.yearid
  FROM halloffame AS h INNER JOIN master AS m ON h.playerid = m.playerid
  WHERE h.inducted = 'Y'
  ORDER BY h.yearid DESC
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT m.namefirst, m.namelast, h.playerid, s.schoolid, h.yearid
  FROM halloffame AS h 
       INNER JOIN master AS m ON h.playerid = m.playerid
       INNER JOIN collegeplaying AS c ON c.playerid = m.playerid
       INNER JOIN schools AS s ON s.schoolid = c.schoolid
  WHERE h.inducted = 'Y' AND s.schoolstate = 'CA'
  ORDER BY h.yearid DESC, s.schoolid, h.playerid ASC
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT h.playerid, m.namefirst, m.namelast, s.schoolid
  FROM halloffame AS h 
       INNER JOIN master AS m ON h.playerid = m.playerid
       LEFT OUTER JOIN collegeplaying AS c ON c.playerid = m.playerid
       LEFT OUTER JOIN schools AS s ON s.schoolid = c.schoolid
  WHERE h.inducted = 'Y'
  ORDER BY h.playerid DESC, h.playerid, s.schoolid ASC
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT m.playerid,
         m.namefirst,
         m.namelast,
         b.yearid,
         (b.h - b.h2b - b.h3b - b.hr + 2*b.h2b + 3*b.h3b + 4*b.hr) 
                / (cast(b.ab as real)) AS slg
  FROM master AS m INNER JOIN batting as b ON m.playerid = b.playerid
  WHERE b.ab > 50
  ORDER BY slg DESC, b.yearid, m.playerid ASC
  LIMIT 10
  
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT m.playerid,
         m.namefirst,
         m.namelast,
         SUM(b.h - b.h2b - b.h3b - b.hr + 2*b.h2b + 3*b.h3b + 4*b.hr)
             / cast(SUM(b.ab) as real) as lslg
  FROM master as m INNER JOIN batting as b on b.playerid = m.playerid
  WHERE b.ab > 0
  GROUP BY m.playerid
  HAVING(SUM(b.ab) > 50)
  ORDER BY lslg DESC, playerid ASC
  LIMIT 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  WITH Q AS (
      SELECT m.playerid, 
             SUM(b.h - b.h2b - b.h3b - b.hr + 2*b.h2b + 3*b.h3b + 4*b.hr)
                / cast(SUM(b.ab) as real) as lslg
      FROM master as m 
           INNER JOIN batting as b on b.playerid = m.playerid
      WHERE b.ab > 0
      GROUP BY m.playerid
      HAVING(SUM(b.ab) > 50))
  SELECT m.namefirst, m.namelast, q.lslg
  FROM master AS m INNER JOIN Q AS q ON m.playerid = q.playerid
  WHERE q.lslg > (SELECT lslg FROM q WHERE playerid = 'mayswi01')
  
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev)
AS
  SELECT yearid, MIN(salary), MAX(salary), AVG(salary), stddev(salary)
  FROM salaries
  GROUP BY yearid
  ORDER BY yearid ASC
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  WITH X AS (SELECT MIN(salary), MAX(salary)
             FROM salaries WHERE yearid = '2016'
  ), Y AS (SELECT i AS binid, 
                  i*(X.max-X.min)/10.0 + X.min AS low,
                  (i+1)*(X.max-X.min)/10.0 + X.min AS high
           FROM generate_series(0,9) AS i, X)
  SELECT binid, low, high, COUNT(*) 
  FROM Y INNER JOIN salaries AS s 
         ON s.salary >= Y.low 
            AND (s.salary < Y.high OR binid = 9 AND s.salary <= Y.high)
            AND yearid = '2016'
  GROUP BY binid, low, high
  ORDER BY binid ASC
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  WITH X AS (SELECT yearid, MIN(salary), MAX(salary), AVG(salary)
             FROM salaries GROUP BY yearid)
  SELECT m2.yearid,
         m2.min - m1.min AS mindiff,
         m2.max - m1.max AS maxdiff,
         m2.avg - m1.avg AS avgdiff
  FROM X AS m1 INNER JOIN X AS m2 ON m2.yearid = m1.yearid + 1
  ORDER BY m2.yearid ASC
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  -- WITH X AS (SELECT yearid, playerid, MAX(salary) OVER(PARTITION BY yearid)
  --            FROM salaries WHERE yearid IN (2000, 2001))
  -- SELECT m.playerid, namefirst, namelast, salary, x.yearid
  -- FROM master AS m 
  --      NATURAL JOIN salaries AS s
  --      INNER JOIN X AS x ON x.playerid = s.playerid
  --                           AND salary = x.max
  --                           AND s.yearid = x.yearid

  -- The above query uses the OVER method but this is not terribly efficient.
  -- A more efficient implementation follows

  WITH X AS(SELECT yearid, MAX(salary) FROM salaries
            WHERE yearid IN (2000,2001)
            GROUP BY yearid)
  SELECT m.playerid, namefirst, namelast, salary, x.yearid
  FROM master AS m 
       NATURAL JOIN salaries AS s
       INNER JOIN X AS x ON salary = x.max AND s.yearid = x.yearid
;
