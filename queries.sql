SELECT rm.RoomCode, Round(Sum((CASE WHEN Datediff(Curdate(), rv.Checkout) <= 180 
AND Datediff(Curdate(), rv.Checkout) >= 0 THEN Datediff(rv.Checkout, rv.Checkin) 
ELSE 0 END) - (CASE WHEN Datediff(Curdate(), rv.Checkin) > 180 AND Datediff( 
Curdate(), rv.Checkout) <= 180 AND Datediff(Curdate(), rv.Checkout) >= 0 THEN 
Datediff(Curdate(), rv.Checkin) - 180 ELSE 0 END) ) / 180, 2) Popularity 
FROM   lab7_rooms rm 
       JOIN lab7_reservations rv 
         ON rm.RoomCode = rv.Room GROUP BY rm.RoomCode;
         
         
         --R1 (Not sorted yet)
         
         select room,
round(sum(datediff(least(checkout, curdate()), greatest(checkin,DATE_SUB(curdate(), Interval 180 Day)))/180),2) as pop

from lab7_reservations
where checkout > DATE_SUB(curdate(), Interval 180 Day) and checkin < curdate()
group by room
order by pop desc;
                         
