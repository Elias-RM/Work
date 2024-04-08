use countries
-- Begin by selecting all columns from the cities table.
select *
from cities

-- Inner join the cities table on the left to the countries table on the right, keeping all of the fields in both tables.
-- You should match the tables on the country_code field in cities and the code field in countries.
-- Do not alias your tables here or in the next step. Using cities and countries is fine for now.
select *
from cities
	INNER JOIN countries
	 on cities.country_code = countries.code;

-- Modify the SELECT statement to keep only the name of the city, the name of the country, and the name of the region the country resides in.
-- Alias the name of the city AS city and the name of the country AS country.
SELECT cities.name AS city, countries.name AS country, region
FROM cities
  INNER JOIN countries
    ON cities.country_code = countries.code;


select *
from countries
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'countries' AND COLUMN_NAME = 'code';
*/

--Join the tables countries (left) and economies (right) aliasing countries AS c and economies AS e.
--Specify the field to match the tables ON.
--From this join, SELECT:
--c.code, aliased as country_code.
--name, year, and inflation_rate, not aliased
/*
select *
from economies;
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'economies' AND COLUMN_NAME = 'code';

SELECT code, REPLACE(code, '"', '') AS CodeSinComillas
FROM economies;
-- economies
UPDATE economies
SET code = REPLACE(code, '"', '');  -- Doble comilla simple para representar una comilla simple en SQL Server
-- languages
UPDATE languages
SET code = REPLACE(code, '"', '');  -- Doble comilla simple para representar una comilla simple en SQL Server
-- countries_plus
UPDATE countries_plus
SET code = REPLACE(code, '"', '');  -- Doble comilla simple para representar una comilla simple en SQL Server
-- currencies
UPDATE currencies
SET code = REPLACE(code, '"', '');  -- Doble comilla simple para representar una comilla simple en SQL Server
-- economies2010
UPDATE economies2010
SET code = REPLACE(code, '"', '');  -- Doble comilla simple para representar una comilla simple en SQL Server
-- economies2015
UPDATE economies2015
SET code = REPLACE(code, '"', '');  -- Doble comilla simple para representar una comilla simple en SQL Server
-- population
UPDATE populations
SET country_code = REPLACE(country_code, '"', '');  -- Doble comilla simple para representar una comilla simple en SQL Server
*/

UPDATE currencies
SET code = REPLACE(code, '"', '');  -- Doble comilla simple para representar una comilla simple en SQL Server


select c.code as country_code, name, year, inflation_rate
FROM countries as c
  Inner JOIN economies as e
    ON c.code = e.code;

-- Inner join countries (left) and populations (right) on the code and country_code fields respectively.
-- Alias countries AS c and populations AS p.
-- Select code, name, and region from countries and also select year and fertility_rate from populations (5 fields in total).

select code, name, region, fertility_rate
from countries as c
inner join populations as p
on c.code = p.country_code;

-- Add an additional INNER JOIN with economies to your previous query by joining on code.
-- Include the unemployment_rate column that became available through joining with economies.
-- Note that year appears in both populations and economies, so you have to explicitly use e.year instead of year as you did before.

select c.code, name, region, fertility_rate, e.year, unemployment_rate
from countries as c
inner join populations as p
on c.code = p.country_code
inner join economies as e
on c.code = e.code;

-- Scroll down the query result and take a look at the results for Albania from your previous query. Does something seem off to you?
-- The trouble with doing your last join on c.code = e.code and not also including year is that e.g. the 2010 value for fertility_rate is also paired with the 2015 value for unemployment_rate.
-- Fix your previous query: in your last ON clause, use AND to add an additional joining condition. In addition to joining on code in c and e, also join on year in e and p.

use countries
select c.code, name, region, fertility_rate, e.year, unemployment_rate
from countries as c
inner join populations as p
on c.code = p.country_code
inner join economies as e
on c.code = e.code and e.year = p.year;

-- Inner join countries on the left and languages on the right with USING(code).
-- Select the fields corresponding to:
-- country name AS country,
-- continent name,
-- language name AS language, and
-- whether or not the language is official.
-- Remember to alias your tables using the first letter of their names.

-- Select fields
Select c.name as country, c.continent, l.name as language, l.official
  -- From countries (alias as c)
  from countries as c
  -- Join to languages (as l)
  inner Join languages as l
    -- Match using code
    on c.code = l.code;

-- Selfjoin
-- Join populations with itself ON country_code.
-- Select the country_code from p1 and the size field from both p1 and p2. SQL won't allow same-named fields, so alias p1.size as size2010 and p2.size as size2015

select p1.country_code, p1.size as size2010, p2.size as size2015
from populations as p1
inner join populations as p2
on p1.country_code = p2.country_code;

-- Notice from the result that for each country_code you have four entries laying out all combinations of 2010 and 2015.
-- Extend the ON in your query to include only those records where the p1.year (2010) matches with p2.year - 5 (2015 - 5 = 2010). This will omit the three entries per country_code that you aren't interested in.

select p1.country_code, p1.size as size2010, p2.size as size2015
from populations as p1
inner join populations as p2
on p1.country_code = p2.country_code
and p1.year = p2.year - 5;

-- As you just saw, you can also use SQL to calculate values like p2.year - 5 for you. With two fields like size2010 and size2015, you may want to determine the percentage increase from one field to the next:
-- With two numeric fields and, the percentage growth from  to can be calculated as 
-- (B_A)/A * 100
-- Add a new field to SELECT, aliased as growth_perc, that calculates the percentage population growth from 2010 to 2015 for each country, using p2.size and p1.size.

-- Select fields with aliases
SELECT p1.country_code,
       p1.size AS size2010, 
       p2.size AS size2015,
       -- Calculate growth_perc
       ((p2.size - p1.size)/p1.size * 100.0) AS growth_perc
-- From populations (alias as p1)
FROM populations AS p1
  -- Join to itself (alias as p2)
  INNER JOIN populations AS p2
    -- Match on country code
    ON p1.country_code = p2.country_code
        -- and year (with calculation)
        AND p1.year = p2.year - 5;

-- Using the countries table, create a new field AS geosize_group that groups the countries into three groups:
-- If surface_area is greater than 2 million, geosize_group is 'large'.
-- If surface_area is greater than 350 thousand but not larger than 2 million, geosize_group is 'medium'.
-- Otherwise, geosize_group is 'small'.

SELECT name, continent, code, surface_area,
    -- First case
    CASE WHEN surface_area > 2000000 THEN 'large'
        -- Second case
        WHEN surface_area > 350000 THEN 'medium'
        -- Else clause + end
        ELSE 'small' END
        -- Alias name
        AS geosize_group
-- From table
FROM countries;

-- Using the populations table focused only for the year 2015, create a new field aliased as popsize_group to organize population size into
-- 'large' (> 50 million),
-- 'medium' (> 1 million), and
-- 'small' groups.
-- Select only the country code, population size, and this new popsize_group as fields

SELECT country_code, size,
    -- First case
    CASE WHEN size > 50000000 THEN 'large'
        -- Second case
        WHEN size > 1000000 THEN 'medium'
        -- Else clause + end
        ELSE 'small' END
        -- Alias name (popsize_group)
        AS popsize_group
-- From table
FROM populations
-- Focus on 2015
WHERE year = 2015;

-- Keep the first query intact that creates pop_plus using INTO.
-- Write a query to join countries_plus AS c on the left with pop_plus AS p on the right matching on the country code fields.
-- Sort the data based on geosize_group, in ascending order so that large appears on top.
-- Select the name, continent, geosize_group, and popsize_group fields.
use countries
SELECT country_code, size,
  CASE WHEN size > 50000000
            THEN 'large'
       WHEN size > 1000000
            THEN 'medium'
       ELSE 'small' END
       AS popsize_group
INTO pop_plus       
FROM populations
WHERE year = 2015;

-- Select all columns of pop_plus

-- Keep the first query intact that creates pop_plus using INTO.
-- Write a query to join countries_plus AS c on the left with pop_plus AS p on the right matching on the country code fields.
-- Sort the data based on geosize_group, in ascending order so that large appears on top.
-- Select the name, continent, geosize_group, and popsize_group fields.

-- Select fields
Select name, continent, geosize_group, popsize_group
-- From countries_plus (alias as c)
from countries_plus as c
  -- Join to pop_plus (alias as p)
  inner Join pop_plus as p
    -- Match on country code
    on c.code = p.country_code
-- Order the table    
order by geosize_group asc;

-- Fill in the code based on the instructions in the code comments to complete the inner join. Note how many records are in the result of the join in the query result.

-- Select the city name (with alias), the country code,
-- the country name (with alias), the region,
-- and the city proper population
SELECT c1.name AS city, code, c2.name AS country,
       region, city_proper_pop
-- From left table (with alias)
FROM cities AS c1
  -- Join to right table (with alias)
  INNER JOIN countries AS c2
    -- Match on country code
    ON c1.country_code = c2.code
-- Order by descending country code
ORDER BY code desc;

-- Perform an inner join and alias the name of the country field as country and the name of the language field as language.
-- Sort based on descending country name.
/*
Select country name AS country, the country's local name,
the language name AS language, and
the percent of the language spoken in the country
*/
use countries
select c.name AS country, local_name, l.name AS language, percentage
-- From left table (alias as c)
FROM countries AS c
  -- Join to right table (alias as l)
  inner JOIN languages AS l
    -- Match on fields
    ON c.code = l.code
-- Order by descending country
ORDER BY country desc;

-- Perform a left join instead of an inner join. Observe the result, and also note the change in the number of records in the result.
-- Carefully review which records appear in the left join result, but not in the inner join result.

/*
Select country name AS country, the country's local name,
the language name AS language, and
the percent of the language spoken in the country
*/
Select c.name AS country, local_name, l.name AS language, percentage
-- From left table (alias as c)
FROM countries AS c
  -- Join to right table (alias as l)
  left JOIN languages AS l
    -- Match on fields
    ON c.code = l.code
-- Order by descending country
ORDER BY country DESC;

-- Begin with a left join with the countries table on the left and the economies table on the right.
-- Focus only on records with 2010 as the year.
-- Select name, region, and gdp_percapita
SELECT name, region, gdp_percapita
-- From countries (alias as c)
FROM countries AS c
  -- Left join with economies (alias as e)
  LEFT JOIN economies AS e
    -- Match on code fields
    ON c.code = e.code
-- Focus on 2010
WHERE e.year = 2010;

-- Modify your code to calculate the average GDP per capita AS avg_gdp for each region in 2010.
-- Select the region and avg_gdp fields.
-- Select fields
SELECT region, avg(gdp_percapita)  AS avg_gdp
-- From countries (alias as c)
FROM countries AS c
  -- Left join with economies (alias as e)
  LEFT JOIN economies AS e
    -- Match on code fields
    ON c.code = e.code
-- Focus on 2010
WHERE e.year = 2010
-- Group by region
GROUP BY region;

-- Arrange this data on average GDP per capita for each region in 2010 from highest to lowest average GDP per capita.
-- Select fields
SELECT region, AVG(gdp_percapita) AS avg_gdp
-- From countries (alias as c)
From countries as c
  -- Left join with economies (alias as e)
  Left join economies as e
    -- Match on code fields
    on c.code = e.code
-- Focus on 2010
where e.year = 2010
-- Group by region
Group by region
-- Order by descending avg_gdp
Order by avg_gdp desc;

-- The left join code is commented out here. Your task is to write a new query using rights joins that produces the same result as what the query using left joins produces. Keep this left joins code commented as you write your own query just below it using right joins to solve the problem.
-- Note the order of the joins matters in your conversion to using right joins!
-- convert this code to use RIGHT JOINs instead of LEFT JOINs
/*
SELECT cities.name AS city, urbanarea_pop, countries.name AS country,
       indep_year, languages.name AS language, percent
FROM cities
  LEFT JOIN countries
    ON cities.country_code = countries.code
  LEFT JOIN languages
    ON countries.code = languages.code
ORDER BY city, language;
*/

SELECT cities.name AS city, urbanarea_pop, countries.name AS country,
       indep_year, languages.name AS language, percentage
FROM languages
  RIGHT JOIN countries
    ON countries.code = languages.code
  RIGHT JOIN cities
    ON cities.country_code = countries.code
ORDER BY city, language;

-- FULL JOIN

-- Choose records in which region corresponds to North America or is NULL.
SELECT name AS country, countries.code, region, basic_unit
-- From countries
FROM countries
  -- Join to currencies
  FULL JOIN currencies
    -- Match on code
    On countries.code = currencies.code
-- Where region is North America or null
WHERE region = 'North America' OR region IS null
-- Order by region
ORDER BY region;

-- Repeat the same query as before, using a LEFT JOIN instead of a FULL JOIN. Note what has changed compared to the FULL JOIN result!
SELECT name AS country, currencies.code, region, basic_unit
-- From countries
FROM countries
  -- Join to currencies
  left Join currencies
    -- Match on code
    On countries.code = currencies.code
-- Where region is North America or null
WHERE region = 'North America' OR region IS NULL
-- Order by region
ORDER BY region;

-- Repeat the same query again but use an INNER JOIN instead of a FULL JOIN. Note what has changed compared to the FULL JOIN and LEFT JOIN results!

SELECT name AS country, currencies.code, region, basic_unit
-- From countries
FROM countries
  -- Join to currencies
  inner Join currencies
    -- Match on code
    On countries.code = currencies.code
-- Where region is North America or null
WHERE region = 'North America' OR region IS NULL
-- Order by region
ORDER BY region;

-- Choose records in which countries.name starts with the capital letter 'V' or is NULL.
-- Arrange by countries.name in ascending order to more clearly see the results.
SELECT countries.name, countries.code, languages.name AS language
-- From languages
FROM languages
  -- Join to countries
  full JOIN countries
    -- Match on code
    On languages.code = countries.code
-- Where countries.name starts with V or is null
WHERE countries.name LIKE 'V%' OR countries.name IS null
-- Order by ascending countries.name
ORDER BY countries.name asc;

-- Repeat the same query as before, using a LEFT JOIN instead of a FULL JOIN. Note what has changed compared to the FULL JOIN result!
SELECT countries.name, languages.code, languages.name AS language
-- From languages
FROM languages
  -- Join to countries
  left Join countries
    -- Match on code
    On languages.code = countries.code
-- Where countries.name starts with V or is null
where countries.name LIKE 'V%' OR countries.name IS NULL
-- Order by ascending countries.name
ORDER BY countries.name;

-- Repeat once more, but use an INNER JOIN instead of a LEFT JOIN. Note what has changed compared to the FULL JOIN and LEFT JOIN results.
SELECT countries.name, languages.code, languages.name AS language
-- From languages
FROM languages
  -- Join to countries
  inner Join countries
    -- Match using code
    On languages.code = countries.code
-- Where countries.name starts with V or is null
WHERE countries.name LIKE 'V%' OR countries.name IS NULL
-- Order by ascending countries.name
Order by countries.name asc;

-- Complete a full join with countries on the left and languages on the right.
-- Next, full join this result with currencies on the right.
-- Use LIKE to choose the Melanesia and Micronesia regions (Hint: 'M%esia').
-- Select the fields corresponding to the country name AS country, region, language name AS language, and basic and fractional units of currency.

-- Select fields (with aliases)
SELECT c1.name AS country, region, l.name AS language,
      basic_unit, frac_unit
-- From countries (alias as c1)
FROM countries AS c1
  -- Join with languages (alias as l)
  FULL JOIN languages AS l
    -- Match on code
    On c1.code = l.code
  -- Join with currencies (alias as c2)
  FULL JOIN currencies AS c2
    -- Match on code
    On c2.code = l.code
-- Where region like Melanesia and Micronesia
WHERE region LIKE 'M%esia';

-- CROSS JOIN
-- Create a CROSS JOIN with cities AS c on the left and languages AS l on the right.
-- Make use of LIKE and Hyder% to choose Hyderabad in both countries.
-- Select only the city name AS city and language name AS language
-- Select fields
use countries
SELECT c.name AS city, l.name AS language
-- From cities (alias as c)
FROM cities AS c        
  -- Join to languages (alias as l)
  CROSS JOIN languages AS l
-- Where c.name like Hyderabad
WHERE c.name LIKE 'Hyder%';

-- Use an INNER JOIN instead of a CROSS JOIN. Think about what the difference will be in the results for this INNER JOIN result and the one for the CROSS JOIN.

-- Select fields
Select c.name as city, l.name as language
-- From cities (alias as c)
from cities as c      
  -- Join to languages (alias as l)
  inner JOIN languages AS l
    -- Match on country code
    on c.country_code = l.code
-- Where c.name like Hyderabad
where c.name like 'Hyder%';

-- Select country name AS country, region, and life expectancy AS life_exp.
-- Make sure to use LEFT JOIN, WHERE, ORDER BY, and LIMIT.

-- Select fields
Select top (5) c.name as country, region, p.life_expectancy as life_exp
-- From countries (alias as c)
from countries as c
  -- Join to populations (alias as p)
  left Join populations as p
    -- Match on country code
    on c.code = p.country_code
-- Focus on 2010
where p.year = 2010
-- Order by life_exp
order by life_exp;

-- UNION
-- Combine the two new tables into one table containing all of the fields in economies2010.
-- Sort this resulting single table by country code and then by year, both in ascending order.

-- Select fields from 2010 table
Select *
  -- From 2010 table
  From economies2010
	-- Set theory clause
	union
-- Select fields from 2015 table
Select *
  -- From 2015 table
  from economies2015
-- Order by code and year
order by code, year;

-- Determine all (non-duplicated) country codes in either the cities or the currencies table. The result should be a table with only one field called country_code.
-- Sort by country_code in alphabetical order.

-- Select field
Select country_code
  -- From cities
  From cities
	-- Set theory clause
	union
-- Select field
Select code
  -- From currencies
  From currencies
-- Order by country_code
order by country_code asc;

-- Determine all combinations (include duplicates) of country code and year that exist in either the economies or the populations tables. Order by code then year.
-- The result of the query should only have two columns/fields. Think about how many records this query should result in.
-- You'll use code very similar to this in your next exercise after the video. Make note of this code after completing it.

-- Select fields
SELECT code, year
  -- From economies
  FROM economies
	-- Set theory clause
	union all
-- Select fields
SELECT country_code, year
  -- From populations
  FROM populations
-- Order by code, year
ORDER BY code, year;

-- Use INTERSECT to determine the records in common for country code and year for the economies and populations tables.
-- Again, order by code and then by year, both in ascending order.
-- Select fields
Select code, year
  -- From economies
  from economies
  -- Set theory clause
  intersect
-- Select fields
Select country_code, year
  -- From populations
  from populations
-- Order by code and year
order by code, year asc;

-- Use INTERSECT to answer this question with countries and cities!
-- Select fields
Select name
  -- From countries
  from countries
	-- Set theory clause
	intersect
-- Select fields
Select name
  -- From cities
  from cities;

-- Order the resulting field in ascending order.
-- Can you spot the city/cities that are actually capital cities which this query misses?
-- Select field
SELECT name
  -- From cities
  FROM cities
	-- Set theory clause
	except
-- Select field
SELECT capital
  -- From countries
  FROM countries
-- Order by result
ORDER BY name;

-- Order by capital in ascending order.
-- The cities table contains information about 236 of the world's most populous cities. The result of your query may surprise you in terms of the number of capital cities that do not appear in this list!
-- Select field
Select capital
  -- From countries
  from countries
	-- Set theory clause
	except
-- Select field
Select name
  -- From cities
  From cities
-- Order by ascending capital
order by capital asc;

-- SEMIJOIN
-- Begin by selecting all country codes in the Middle East as a single field result using SELECT, FROM, and WHERE.
use countries
-- Select code
Select code
  -- From countries
  from countries
-- Where region is Middle East
where region = 'Middle East'

-- Below the commented code, select only unique languages by name appearing in the languages table.
-- Order the resulting single field table by name in ascending order

-- Select field
SELECT DISTINCT name
  -- From languages
  FROM languages
-- Order by name
ORDER BY name;

-- Combine the previous two queries into one query by adding a WHERE IN statement to the SELECT DISTINCT query to determine the unique languages spoken in the Middle East.
-- Order the result by name in ascending order.

-- Query from step 2
SELECT DISTINCT name
  FROM languages
-- Where in statement
where code in
  -- Query from step 1
  -- Subquery
  (SELECT code
   FROM countries
   WHERE region = 'Middle East')
-- Order by name
order by name asc;

-- Begin by determining the number of countries in countries that are listed in Oceania using SELECT, FROM, and WHERE.

-- Select statement
Select count(continent) as continentcount 
  -- From countries
  from countries
-- Where continent is Oceania
where continent = 'Oceania';

-- Complete an inner join with countries AS c1 on the left and currencies AS c2 on the right to get the different currencies used in the countries of Oceania.
-- Match ON the code field in the two tables.

-- Select fields (with aliases)
Select c1.code, c1.name, basic_unit as currency
  -- From countries (alias as c1)
  from countries as c1
  	-- Join with currencies (alias as c2)
  	inner Join currencies as c2
    -- Match on code
    on c1.code = c2.code
-- Where continent is Oceania
where continent = 'Oceania';

-- Use NOT IN and (SELECT code FROM currencies) as a subquery to get the country code and country name for the Oceanian countries that are not included in the currencies table.
-- Select fields
Select code, continent, name
  -- From Countries
  From Countries
  -- Where continent is Oceania
  Where continent = 'Oceania'
  	-- And code not in
  	and code not in
  	-- Subquery
  	(Select code
  	 From currencies);

-- Identify the country codes that are included in either economies or currencies but not in populations.
-- Use that result to determine the names of cities in the countries that match the specification in the previous instruction.

-- Select the city name
Select name
  -- Alias the table where city name resides
  from cities AS c1
  -- Choose only records matching the result of multiple set theory clauses
  WHERE country_code IN
(
    -- Select appropriate field from economies AS e
    SELECT e.code
    FROM economies AS e
    -- Get all additional (unique) values of the field from currencies AS c2  
    union all
    SELECT c2.code
    FROM currencies AS c2
    -- Exclude those appearing in populations AS p
    except
    SELECT p.country_code
    FROM populations AS p
);

-- Begin by calculating the average life expectancy across all countries for 2015.
-- Select average life_expectancy
Select avg(life_expectancy) as promediodevida
  -- From populations
  From populations
-- Where year is 2015
where year = 2015

-- Select all fields from populations with records corresponding to larger than 1.15 times the average you calculated in the first task for 2015. In other words, change the 100 in the example above with a subquery.

-- Select fields
SELECT *
  -- From populations
  FROM populations
-- Where life_expectancy is greater than
WHERE life_expectancy >
  -- 1.15 * subquery
  1.15 * (SELECT AVG(life_expectancy)
   FROM populations
   WHERE year = 2015) AND
  year = 2015;

-- Make use of the capital field in the countries table in your subquery.
-- Select the city name, country code, and urban area population fields.

-- Select fields
Select name, country_code, urbanarea_pop
  -- From cities
  from cities
-- Where city name in the field of capital cities
where name IN
  -- Subquery
  (Select capital
   from countries)
ORDER BY urbanarea_pop DESC;

SELECT countries.name as country, COUNT(*) AS cities_num
  FROM cities
    INNER JOIN countries
    ON countries.code = cities.country_code
GROUP BY country
ORDER BY cities_num DESC, country;

-- Begin by determining for each country code how many languages are listed in the languages table using SELECT, FROM, and GROUP BY.
-- Alias the aggregated field as lang_num.
-- Select fields (with aliases)
Select code, count(name) as lang_num
  -- From languages
  from languages
-- Group by code
Group by code;

-- Create an INNER JOIN with countries on the left and economies on the right with USING, without aliasing your tables or columns.
-- Retrieve the country's name, continent, and inflation rate for 2015.
-- Select fields
Select name, continent, inflation_rate
  -- From countries
  from countries
  	-- Join to economies
  	inner Join economies
    -- Match on code
    On countries.code = economies.code
-- Where year is 2015
Where year = 2015;

-- Select the maximum inflation rate in 2015 AS max_inf grouped by continent using the previous step's query as a subquery in the FROM clause.
-- Thus, in your subquery you should:
-- Create an inner join with countries on the left and economies on the right with USING (without aliasing your tables or columns).
-- Retrieve the country name, continent, and inflation rate for 2015.
-- Alias the subquery as subquery.
-- This will result in the six maximum inflation rates in 2015 for the six continents as one field table. Make sure to not include continent in the outer SELECT statement.

-- Select the maximum inflation rate as max_inf
Select max(inflation_rate) as max_inf
  -- Subquery using FROM (alias as subquery)
  FROM (
      SELECT name, continent, inflation_rate
      FROM countries
      INNER JOIN economies
      On countries.code = economies.code
      WHERE year = 2015) AS subquery
-- Group by continent
Group by continent;


-- Now it's time to append your second query to your first query using AND and IN to obtain the name of the country, its continent, and the maximum inflation rate for each continent in 2015.
-- For the sake of practice, change all joining conditions to use ON instead of USING.

-- Select fields
SELECT name, continent, inflation_rate
  -- From countries
  FROM countries
	-- Join to economies
	INNER JOIN economies
	-- Match on code
	on countries.code = economies.code
  -- Where year is 2015
  WHERE year = 2015
    -- And inflation rate in subquery (alias as subquery)
    And inflation_rate in (
        SELECT MAX(inflation_rate) AS max_inf
        FROM (
             SELECT name, continent, inflation_rate
             FROM countries
             INNER JOIN economies
             on countries.code = economies.code
             WHERE year = 2015) AS subquery
      -- Group by continent
        GROUP BY continent);

-- Select the country code, inflation rate, and unemployment rate.
-- Order by inflation rate ascending.
-- Do not use table aliasing in this exercise.
-- Select fields
SELECT code, inflation_rate, unemployment_rate
  -- From economies
  FROM economies
  -- Where year is 2015 and code is not in
  WHERE year = 2015 AND code not in
  	-- Subquery
  	(SELECT code
  	 FROM countries
  	 WHERE (gov_form= 'Constitutional Monarchy' OR gov_form LIKE '%Republic%'))
-- Order by inflation rate
ORDER BY inflation_rate;

-- FINAL CHALLENGE
-- Select unique country names. Also select the total investment and imports fields.
-- Use a left join with countries on the left. (An inner join would also work, but please use a left join here.)
-- Match on code in the two tables AND use a subquery inside of ON to choose the appropriate languages records.
-- Order by country name ascending.
-- Use table aliasing but not field aliasing in this exercise.

-- Select fields
SELECT DISTINCT name, total_investment, imports
  -- From table (with alias)
  FROM countries AS c
    -- Join with table (with alias)
    LEFT JOIN economies AS e
      -- Match on code
      ON (c.code = e.code
      -- and code in Subquery
        AND c.code IN (
          SELECT l.code
          FROM languages AS l
          WHERE official = 'true'
        ) )
  -- Where region and year are correct
  WHERE region = 'Central America' AND year = 2015
-- Order by field
ORDER BY name asc;

-- Include the name of region, its continent, and average fertility rate aliased as avg_fert_rate.
-- Sort based on avg_fert_rate ascending.
-- Remember that you'll need to GROUP BY all fields that aren't included in the aggregate function of SELECT.

use countries
-- Select fields
SELECT region, continent, avg(fertility_rate) AS avg_fert_rate
  -- From left table
  FROM countries AS c
    -- Join to right table
    INNER JOIN populations AS p
      -- Match on join condition
      ON c.code = p.country_code
  -- Where specific records matching some condition
  WHERE year = 2015
-- Group appropriately
GROUP BY region, continent 
-- Order appropriately
ORDER BY avg_fert_rate asc;

-- Select the city name, country code, city proper population, and metro area population.
-- Calculate the percentage of metro area population composed of city proper population for each city in cities, aliased as city_perc.
-- Focus only on capital cities in Europe and the Americas in a subquery.
-- Make sure to exclude records with missing data on metro area population.
-- Order the result by city_perc descending.
-- Then determine the top 10 capital cities in Europe and the Americas in terms of this city_perc percentage.

-- Select fields
SELECT top(10) name, country_code, city_proper_pop, metroarea_pop,  
      -- Calculate city_perc
      city_proper_pop	/ metroarea_pop * 100 AS city_perc
  -- From appropriate table
  FROM cities
  -- Where 
  WHERE name IN
    -- Subquery
    (SELECT capital
     FROM countries
     WHERE (continent = 'Europe'
        OR continent LIKE '%America'))
       AND metroarea_pop IS NOT NULL
-- Order appropriately
ORDER BY city_perc desc



