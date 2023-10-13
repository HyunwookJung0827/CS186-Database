-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;

-- Question 0
CREATE VIEW q0(era)
AS
  SELECT MAX(ERA)
  FROM Pitching
; -- Done

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM People
  WHERE weight > 300
; -- Done

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  From People
  WHERE namefirst LIKE '% %'
  Order by namefirst, namelast
; -- Done

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*)
  FROM People
  Group by birthyear
  Order by birthyear
; -- Done

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*)
  FROM People
  Group by birthyear
  HAVING AVG(height) > 70
  Order by birthyear
; -- Done

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT namefirst, namelast, p.playerid, yearid
  FROM People p Inner JOin HallofFame h ON p.playerid = h.playerid
  WHERE h.inducted = 'Y'
  Order by yearid desc, p.playerid
; -- Done

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT namefirst, namelast, q2i.playerid, s.schoolid, q2i.yearid
  FROM q2i, Schools s, CollegePlaying c
  WHERE c.playerid = q2i.playerid and s.schoolstate = 'CA' and s.schoolid = c.schoolid
  Order by yearid desc, s.schoolid, q2i.playerid
; -- Done

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT q2i.playerid, q2i.namefirst, q2i.namelast, c.schoolid
  FROM q2i LEFT OUTER JOIN CollegePlaying c ON q2i.playerid = c.playerid
  Order by q2i.playerid desc, c.schoolid
; -- Done

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT p.playerid, namefirst, namelast, b.yearid, (H+H2B+2.0*H3B+3.0*HR) / AB AS slg
  FROM People p INNER JOIN Batting b ON p.playerid = b.playerid
  WHERE AB > 50
  Order by slg desc, b.yearid, b.playerid
  LIMIT 10
; -- Done

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT p.playerid, namefirst, namelast, (SUM(H)+SUM(H2B)+2.0*SUM(H3B)+3.0*SUM(HR)) / SUM(AB) AS lslg
  FROM People p INNER JOIN Batting b ON p.playerid = b.playerid
  Group by p.playerid
  HAVING SUM(AB) > 50
  Order by lslg desc, b.playerid
  LIMIT 10
; -- Done

-- Question 3iii

CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT namefirst, namelast, (SUM(H)+SUM(H2B)+2.0*SUM(H3B)+3.0*SUM(HR)) / SUM(AB) AS lslg
  FROM People p INNER JOIN Batting b ON p.playerid = b.playerid
  Group by p.playerid
  HAVING SUM(AB) > 50 and lslg > (SELECT (SUM(H)+SUM(H2B)+2.0*SUM(H3B)+3.0*SUM(HR)) / SUM(AB) FROM Batting Group by playerid having playerid = 'mayswi01')
  Order by lslg desc, b.playerid
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg)
AS
  SELECT yearid, min(salary), max(salary), avg(salary)
  FROM Salaries s
  GROUP By yearid
  Order by yearid
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  WITH SR AS (SELECT min(salary) as min, max(salary) as max, max(salary) - min(salary) AS salaryrange FROM Salaries where yearid = 2016),
  BIN AS (SELECT cast((10 * (SALARY - MIN) / SALARYRANGE) as int) AS BINID, MIN + (SALARYRANGE / 10)*cast((10 * (SALARY - MIN) / SALARYRANGE) as int) AS LOW, MIN + (SALARYRANGE / 10)*(cast((10 * (SALARY - MIN) / SALARYRANGE) as int)+1) AS HIGH FROM SALARIES s, SR where s.yearid = 2016 and salary != max),
  MAXBIN AS (SELECT 9 AS BINID, MIN + (SALARYRANGE / 10)*9 AS low, MAX AS high FROM SALARIES s, SR where s.yearid = 2016 and salary = max),
  FULLBIN AS (SELECT BINID, low, high FROM BIN UNION ALL SELECT BINID, low, high FROM MAXBIN),
  IDLH AS (SELECT BINID, MIN+(SALARYRANGE/10)*BINID AS low, MIN+(SALARYRANGE/10)*(BINID+1) as high FROM BINIDS, SR)
  SELECT IDLH.BINID, IDLH.low, IDLH.high, COUNT(*)
  FROM IDLH LEFT OUTER JOIN FULLBIN ON IDLH.BINID = FULLBIN.BINID and IDLH.low = FULLBIN.low and IDLH.high = FULLBIN.high
  GROUP BY IDLH.BINID
  ORDER BY IDLH.BINID
  --LEFTFULLBIN AS (SELECT BIN.BINID, BIN.low, BIN.high, COUNT(*) AS COUNT FROM BIN LEFT OUTER JOIN MAXBIN ON BIN.BINID = MAXBIN.BINID AND bin.low = MAXBIN.low and bin.high = MAXBIN.high GROUP BY BIN.BINID, BIN.low, BIN.high),
    --RIGHTFULLBIN AS (SELECT BIN.BINID, BIN.low, BIN.high, COUNT(*) AS COUNT FROM MAXBIN LEFT OUTER JOIN BIN ON BIN.BINID = MAXBIN.BINID AND bin.low = MAXBIN.low and bin.high = MAXBIN.high GROUP BY BIN.BINID, BIN.low, BIN.high),
    --WITH sr AS (SELECT min, max, max - min AS salaryrange FROM q4i GROUP By yearid HAVING yearid = 2016)
  --WITH bin AS (SELECT binids.binid, min + salaryrange / 10 * binid as low, min + salaryrange / 10 * (binid + 1) as high FROM binids, sr)
  --SELECT binid, low, high, count(*)
  --FROM Salaries, bin
  --WHERE Salary between low and high
; -- Done

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  WITH NEXTYEARBOOK AS (SELECT yearid, min(salary) as min, max(salary) as max, avg(salary) as avg FROM SALARIES GROUP BY YEARID),
  YEARBOOK AS (SELECT YEARID + 1 AS YEARID, MIN, MAX, AVG FROM NEXTYEARBOOK)
  SELECT NEXTYEARBOOK.yearid, NEXTYEARBOOK.MIN - YEARBOOK.MIN AS mindiff, NEXTYEARBOOK.MAX - YEARBOOK.MAX AS maxdiff, NEXTYEARBOOK.AVG - YEARBOOK.AVG AS avgdiff
  FROM NEXTYEARBOOK INNER JOIN YEARBOOK ON NEXTYEARBOOK.YEARID = YEARBOOK.YEARID  -- replace this line
  ORDER BY NEXTYEARBOOK.YEARID
; -- Done

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  WITH MAXSALARIES AS (SELECT MAX(SALARY) AS MAXSALARY, YEARID FROM SALARIES GROUP BY YEARID HAVING YEARID=2000 OR YEARID=2001),
  ZERO AS (SELECT playerid, salary, SALARIES.yearid FROM SALARIES INNER JOIN MAXSALARIES ON SALARIES.YEARID = MAXSALARIES.YEARID AND SALARIES.SALARY = MAXSALARIES.MAXSALARY WHERE SALARIES.YEARID = 2000),
  ONE AS (SELECT playerid, salary, SALARIES.yearid FROM SALARIES INNER JOIN MAXSALARIES ON SALARIES.YEARID = MAXSALARIES.YEARID AND SALARIES.SALARY = MAXSALARIES.MAXSALARY WHERE SALARIES.YEARID = 2001),
  LEADERBOARD AS
    (SELECT playerid, salary, yearid FROM ZERO
    UNION
    SELECT playerid, salary, yearid FROM ONE)
  SELECT LEADERBOARD.playerid, namefirst, namelast, LEADERBOARD.salary, LEADERBOARD.yearid
  FROM LEADERBOARD INNER JOIN PEOPLE ON LEADERBOARD.PLAYERID = PEOPLE.PLAYERID
; --Done
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  WITH ALLSTARS AS (SELECT ALLSTARFULL.PLAYERID, ALLSTARFULL.TEAMID, SALARIES.SALARY FROM ALLSTARFULL INNER JOIN SALARIES ON ALLSTARFULL.PLAYERID = SALARIES.PLAYERID AND ALLSTARFULL.YEARID = SALARIES.YEARID WHERE ALLSTARFULL.YEARID = 2016)
  SELECT TEAMID, MAX(SALARY) - MIN(SALARY) AS diffAvg
  FROM ALLSTARS
  GROUP BY TEAMID
;

