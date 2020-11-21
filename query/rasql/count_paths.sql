-- hint: shuffle

create table rc(A integer, B integer);

WITH recursive paths (X, count() AS Cnt)
AS  (SELECT {startvertex}, 1)
        UNION
    (SELECT rc.B, paths.Cnt FROM paths, rc
     WHERE paths.X = rc.A)
SELECT X, Cnt FROM paths
