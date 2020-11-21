-- hint: broadcast

create table rc(A integer, B integer);

WITH recursive paths (X, Y, count() AS Cnt)
AS  (SELECT A, B, 1 FROM rc)
        UNION
    (SELECT X, B, Cnt FROM paths, rc
     WHERE paths.Y = rc.A)
SELECT X, Y, Cnt FROM paths
