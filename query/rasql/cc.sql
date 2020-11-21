-- hint: shuffle

create table arc(X integer, Y integer);

WITH recursive cc (X, min() AS CmpId)
AS  (SELECT X, X FROM arc)
        UNION
    (SELECT arc.Y, cc.CmpId FROM cc, arc
     WHERE cc.X = arc.X)
-- comment: which nodes are populated the final cc group id by 2 hops
SELECT a2.Y
FROM cc, arc a1, arc a2
WHERE cc.X = a2.Y and cc.CmpId = a1.X and a1.Y = a2.X
