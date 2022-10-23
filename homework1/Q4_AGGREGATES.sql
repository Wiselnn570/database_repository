SELECT CategoryName ||'|'||COUNT(CategoryName)||'|'||ROUND(AVG(UnitPrice), 2)||'|'||MIN(UnitPrice)||'|'||MAX(UnitPrice)||'|'||SUM(UnitsOnOrder)
AS Q4_ANSWER
FROM Product, Category
WHERE Product.CategoryId=Category.Id
GROUP BY Category.Id
HAVING COUNT(*) > 10
ORDER BY Category.Id;