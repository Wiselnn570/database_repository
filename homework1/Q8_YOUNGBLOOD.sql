SELECT RegionDescription||'|'||FirstName||'|'||LastName||'|'||BirthDate
AS Q8_ANSWER
FROM
(SELECT *
    FROM Employee
    LEFT OUTER JOIN EmployeeTerritory ON EmployeeId=Employee.Id
    LEFT OUTER JOIN Territory ON TerritoryId=Territory.Id
    LEFT OUTER JOIN Region ON RegionId=Region.Id
    ORDER BY BirthDate DESC)
GROUP BY RegionId
ORDER BY RegionId;