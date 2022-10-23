SELECT ComPanyName || '|' || printf("%.2f",COUNT(CASE WHEN ShippedDate>RequiredDate THEN 1 ELSE null END) * 100 / ROUND(COUNT('Order'.Id)))
AS Q3_ANSWER
FROM 'Order', Shipper
WHERE 'Order'.ShipVia = Shipper.Id
GROUP BY Shipper.Id
ORDER BY COUNT(CASE WHEN ShippedDate>RequiredDate THEN 1 ELSE null END) * 100 / ROUND(COUNT('Order'.Id)) DESC;