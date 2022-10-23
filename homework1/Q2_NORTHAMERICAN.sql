
SELECT Id || '|' || ShipCountry || '|' || (case WHEN ShipCountry IN ('USA','Mexico','Canada') THEN "NorthAmerica" ELSE "OtherPlace" END)
AS Q2_ANSWER
FROM "Order"
WHERE Id>=15445
LIMIT 20;