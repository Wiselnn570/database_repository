SELECT DISTINCT ShipName || '|' || substr(ShipName, 1, instr(ShipName, '-')-1)
AS Q1_ANSWER
FROM 'Order'
WHERE ShipName LIKE '%-%'
ORDER BY ShipName;