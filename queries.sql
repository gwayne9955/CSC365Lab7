SELECT rm.RoomCode, Round(Sum((CASE WHEN Datediff(Curdate(), rv.Checkout) <= 180 
AND Datediff(Curdate(), rv.Checkout) >= 0 THEN Datediff(rv.Checkout, rv.Checkin) 
ELSE 0 END) - (CASE WHEN Datediff(Curdate(), rv.Checkin) > 180 AND Datediff( 
Curdate(), rv.Checkout) <= 180 AND Datediff(Curdate(), rv.Checkout) >= 0 THEN 
Datediff(Curdate(), rv.Checkin) - 180 ELSE 0 END) ) / 180, 2) Popularity 
FROM   lab7_rooms rm 
       JOIN lab7_reservations rv 
         ON rm.RoomCode = rv.Room GROUP BY rm.RoomCode;
