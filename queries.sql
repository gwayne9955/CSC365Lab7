select rm.RoomCode, 
round(sum((case when datediff(CURDATE(), rv.Checkout) <= 180 
    and datediff(CURDATE(), rv.Checkout) >= 0
                then datediff(rv.Checkout, rv.Checkin) 
                else 0 
                end) - 
                (case when datediff(CURDATE(), rv.Checkin) > 180 and datediff(CURDATE(), rv.Checkout) <= 180
                then datediff(CURDATE(), rv.Checkin) - 180
                else 0
                end)
                ) / 180, 2) Popularity 
from lab7_rooms rm join lab7_reservations rv on rm.RoomCode = rv.Room
group by rm.RoomCode;
