SELECT CompanyName || '|' || CustomerId || '|' || ROUND(total, 2)
AS Q7_ANSWER
FROM (SELECT IFNULL(CompanyName, "MISSING_NAME") AS CompanyName, CustomerId, total, NTILE(4) OVER(ORDER BY CAST(total AS float)) AS quartile
    FROM
    (
        (SELECT 'Order'.CustomerId, SUM(UnitPrice*Quantity) AS total
            FROM 'Order'
            LEFT OUTER JOIN OrderDetail ON 'Order'.Id=OrderDetail.OrderId
            GROUP BY 'Order'.CustomerId)
        LEFT OUTER JOIN Customer ON CustomerId=Customer.Id
    )
)
WHERE quartile=1
ORDER BY CAST(total AS float);
