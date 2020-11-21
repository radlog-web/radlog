-- hint: shuffle

create table rc(A integer, B integer);

WITH recursive reach (Node)
AS  (SELECT {startvertex})
        UNION
    (SELECT rc.B FROM reach, rc
     WHERE reach.Node = rc.A)
SELECT Node FROM reach
