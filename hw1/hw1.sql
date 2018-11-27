DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv;

-- Question 0
CREATE VIEW q0(era) AS
select max(era) from pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear) AS

;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear) AS
;


-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count) AS

;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count) AS

;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid) AS

;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid) AS

;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid) AS

;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg) AS
  
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg) AS
  
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg) AS

;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev) AS
  
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count) AS

;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff) AS
 
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid) AS
  
;
