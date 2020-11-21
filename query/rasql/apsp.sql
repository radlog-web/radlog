-- hint: broadcast

create table warc(From integer, To integer, Cost integer);

WITH recursive sp (From, To, min() AS Cost)
AS  (SELECT From, To, Cost FROM warc)
        UNION
    (SELECT sp.From, warc.To, sp.Cost + warc.Cost FROM sp, warc
     WHERE sp.To = warc.From)
SELECT From, To, Cost FROM sp
