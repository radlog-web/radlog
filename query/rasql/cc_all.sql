-- hint: shuffle

create table arc(X integer, Y integer);

WITH recursive cc (X, min() AS CmpId)
AS  (SELECT X, X FROM arc)
        UNION
    (SELECT arc.Y, cc.CmpId FROM cc, arc
     WHERE cc.X = arc.X)
-- comment: node and its cc group id
SELECT X, CmpId FROM cc
