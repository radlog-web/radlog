-- hint: shuffle

create table warc(From integer, To integer, Cost integer);

WITH recursive sp (To, min() AS Cost)
AS  (SELECT {startvertex}, 0)
        UNION
    (SELECT warc.To, sp.Cost + warc.Cost FROM sp, warc
     WHERE sp.To = warc.From)
SELECT To, Cost FROM sp
