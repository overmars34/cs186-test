DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv;

-- Question 0
CREATE VIEW q0(era) AS
select max(era)
from pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear) AS
select namefirst, namelast, birthyear 
from master 
where weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear) AS
select namefirst, namelast, birthyear 
from master 
where namefirst like '% %'
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count) AS
select birthyear, avg(height), count(*) 
from master 
group by birthyear 
order by birthyear asc
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count) AS
select birthyear, avg(height), count(*)
from master
group by birthyear
having avg(height) > 70
order by count(*)
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid) AS
select m.namefirst, m.namelast, h.playerid, h.yearid
from master m, halloffame h
where h.playerid = m.playerid
and h.inducted = 'Y'
order by yearid desc
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid) AS
select distinct m.namefirst, m.namelast, m.playerid, c.schoolid, h.yearid
from master m, collegeplaying c, halloffame h, schools s
where m.playerid = c.playerid and m.playerid = h.playerid and s.schoolid = c.schoolid
and h.inducted = 'Y'
and s.schoolstate = 'CA'
order by h.yearid desc, c.schoolid asc, m.playerid asc
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid) AS
select distinct c.playerid,
(select m.namefirst from master m where m.playerid = c.playerid),
(select m.namelast from master m where m.playerid = c.playerid),
c.schoolid
from collegeplaying c, halloffame h
where c.playerid = h.playerid
and h.inducted = 'Y'
order by c.playerid desc, c.schooid asc
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg) AS
select b.playerid, m.namefirst, m.namelast, b.yearid,  b.h, b.h2b, b.h3b, b.hr, b.ab, (b.h + 2*b.h2b + 3*b.h3b +4*hr)/b.ab
from batting b, master m
where b.playerid = m.playerid
and b.ab > 50
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
