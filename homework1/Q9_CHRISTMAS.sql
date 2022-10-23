SELECT GROUP_CONCAT(ProductName,", ") AS Q9_ANWSER
FROM 'Order'
         LEFT OUTER JOIN OrderDetail ON "Order".Id = OrderDetail.OrderId
         LEFT OUTER JOIN Product ON OrderDetail.ProductId = Product.Id
         LEFT OUTER JOIN Customer ON "Order".CustomerId = Customer.Id
WHERE Customer.CompanyName = 'Queen Cozinha' AND 'Order'.OrderDate LIKE '2014-12-25 __:__:__';