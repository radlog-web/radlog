-- hint: shuffle

create table report(Mgr integer, Emp integer);

WITH recursive dept(Mgr, count() as Cnt)
AS  (SELECT report.Emp, 1 FROM report)
         UNION
    (SELECT report.Mgr, dept.Cnt
     FROM dept, report
     WHERE dept.Mgr = report.Emp)
SELECT Mgr, Cnt FROM dept
