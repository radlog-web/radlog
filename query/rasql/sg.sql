-- hint: broadcast

create table rel(Parent integer, Child integer);

WITH recursive sg (X, Y)
AS  (SELECT a.Child, b.Child FROM rel a, rel b
     WHERE a.Parent = b.Parent AND a.Child <> b.Child)
        UNION
    (SELECT a.Child, b.Child FROM rel a, sg, rel b
     WHERE a.Parent = sg.X AND b.Parent = sg.Y)
SELECT X, Y FROM sg
