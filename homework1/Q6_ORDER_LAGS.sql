SELECT Id||"|"||OrderDate||"|"||(LAG(OrderDate,1, OrderDate) OVER(ORDER BY OrderDate))||"|"||
ROUND(julianday(OrderDate)-julianday(LAG(OrderDate,1, OrderDate) OVER(ORDER BY OrderDate)),2)
AS Q6_ANSWER
FROM 'Order'
WHERE CustomerId="BLONP"
ORDER BY OrderDate
LIMIT 10;