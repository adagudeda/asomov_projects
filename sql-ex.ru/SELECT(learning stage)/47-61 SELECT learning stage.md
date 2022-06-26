## 47

Определить страны, которые потеряли в сражениях все свои корабли.

```sql
WITH all_ships AS (
SELECT ship, ship as class
FROM Outcomes
UNION
SELECT name, class
FROM Ships),

all_ships_country AS (
SELECT country, count(*) as number
FROM Classes c
JOIN all_ships alls ON c.class = alls.class
GROUP BY country),

country_sunk_ship AS ( 
SELECT country, count(*) as number
FROM Outcomes o
LEFT JOIN Ships s ON o.ship = s.name
JOIN Classes c ON c.class = s.class OR c.class = o.ship
WHERE result = 'sunk'
GROUP BY country
)

SELECT country
FROM(
  SELECT *
  FROM country_sunk_ship
  INTERSECT
  SELECT *
  FROM all_ships_country) final

```

## 48

Найдите классы кораблей, в которых хотя бы один корабль был потоплен в сражении.

```sql
SELECT DISTINCT c.class
FROM outcomes o
LEFT JOIN ships s ON o.ship = s.name
JOIN Classes c ON c.class = s.class OR c.class = o.ship
WHERE result = 'sunk'
```

## 49

Найдите названия кораблей с орудиями калибра 16 дюймов (учесть корабли из таблицы `Outcomes`).

```sql
SELECT name
FROM Classes c
JOIN Ships s ON c.class = s.class
WHERE bore = 16
UNION
SELECT class
FROM Classes c
JOIN Outcomes o ON c.class = o.ship
WHERE bore = 16
```

## 50

Найдите сражения, в которых участвовали корабли класса Kongo из таблицы `Ships`.

```sql
SELECT DISTINCT o.battle
FROM Outcomes o
JOIN Ships s ON s.name = o.ship
WHERe class = 'Kongo'
```

## 51

Найдите названия кораблей, имеющих наибольшее число орудий среди всех имеющихся кораблей такого же водоизмещения (учесть корабли из таблицы `Outcomes`).

```sql
WITH all_ships as (
SELECT ship, displacement, numGuns
FROM Outcomes o
JOIN Classes c ON c.class = o.ship
UNION
SELECT name, displacement, numGuns
FROM Ships s
JOIN Classes c ON c.class = s.class),

max_guns as (
SELECT displacement, MAX(numGuns) as maxg
FROM all_ships
GROUP BY displacement)

SELECT ship
FROM all_ships alls
JOIN max_guns mg ON alls.displacement= mg.displacement
        AND alls.numGuns = mg.maxg
```

## 52

Определить названия всех кораблей из таблицы `Ships`, которые могут быть линейным японским кораблем,
имеющим число главных орудий не менее девяти, калибр орудий менее 19 дюймов и водоизмещение не более 65 тыс.тонн

```sql
SELECT name
FROM Ships s
JOIN Classes c ON c.class = s.class
WHERE country = 'Japan'
  AND (numGuns >= 9 or numGuns IS NULL)
  AND (bore < 19 OR bore IS NULL)
  AND (displacement <= 65000 OR displacement IS NULL)
  AND type = 'bb'
```

## 53

Определите среднее число орудий для классов линейных кораблей.
Получить результат с точностью до 2-х десятичных знаков.

- NOTE способ округления и преобразования типов. Функция `ROUND` не применяется.

```sql
SELECT CAST(AVG(numGuns*1.0) AS NUMERIC(4,2))
FROM classes
WHERE type = 'bb'
```

## 54

С точностью до 2-х десятичных знаков определите среднее число орудий всех линейных кораблей (учесть корабли из таблицы `Outcomes`).

```sql
WITH tab AS 
(SELECT c.class, ship, type, numGuns
FROM Classes c
JOIN (SELECT DISTINCT ship
      FROM Outcomes) o ON c.class = o.ship
UNION
SELECT c.class, name, type, numGuns
FROM Classes c
JOIN Ships s ON c.class = s.class)

SELECT CAST(AVG(numGuns*1.0) AS NUMERIC(4, 2))
FROM tab
WHERE type = 'bb'
```

## 55

Для каждого класса определите год, когда был спущен на воду первый корабль этого класса. Если год спуска на воду головного корабля неизвестен, определите минимальный год спуска на воду кораблей этого класса. Вывести: класс, год.

```sql
SELECT c.class, MIN(launched)
FROM Classes c
LEFT JOIN Ships s ON c.class = s.class
LEFT JOIN Outcomes o ON c.class = o.ship
GROUP BY c.class
```

## 56

Для каждого класса определите число кораблей этого класса, потопленных в сражениях. Вывести: класс и число потопленных кораблей.

```sql
SELECT c.class, COUNT(DISTINCT ship) sunked 
FROM Classes c 
LEFT JOIN Ships s ON c.class = s.class 
LEFT JOIN 
 (SELECT ship 
 FROM Outcomes 
 WHERE result = 'sunk'
 ) o ON o.ship = s.name OR o.ship = c.class 
GROUP BY c.class
```

## 57

Для классов, имеющих потери в виде потопленных кораблей и не менее 3 кораблей в базе данных, вывести имя класса и число потопленных кораблей.

```sql
WITH all_ships AS (
SELECT c.class, name
FROM classes c
JOIN ships s ON c.class = s.class
UNION
SELECT c.class, ship
FROM Outcomes o
JOIN Classes c ON c.class = o.ship
),

sunk_ships AS (
SELECT c.class, COUNT(*) cnt
FROM outcomes o
LEFT JOIN ships s ON o.ship = s.name
JOIN classes c ON o.ship = c.class OR c.class = s.class
WHERE result = 'sunk'
GROUP BY c.class)

SELECT * 
FROM sunk_ships
WHERE class IN (SELECT class
            FROM all_ships 
            GROUP BY class
            HAVING COUNT(*) >=3)
```

## 58

Для каждого типа продукции и каждого производителя из таблицы `Product` c точностью до двух десятичных знаков найти процентное отношение числа моделей данного типа данного производителя к общему числу моделей этого производителя.
Вывод: `maker`, `type`, процентное отношение числа моделей данного типа к общему числу моделей производителя

```sql
WITH maker_models AS (
SELECT pt.maker, pt.type, p.model
FROM (
      select distinct a.maker, b.type
      from product a, product b
      ) pt
LEFT JOIN product p ON pt.maker=p.maker and pt.type=p.type)

SELECT DISTINCT maker, type,
    CAST(ROUND((
    COUNT(model) OVER(partition by maker, type))*100.0/
    COUNT(model) OVER(partition by maker)
    ,2) as NUMERIC(5,2)) as fin
FROM maker_models
```

## 59

Посчитать остаток денежных средств на каждом пункте приема
для базы данных с отчетностью не чаще одного раза в день.
Вывод: пункт, остаток.

```sql
WITH inc as (
SELECT point, SUM(inc) as inc
FROM Income_o inc
GROUP BY point),

out as (
SELECT point, SUM(out) as out
FROM Outcome_o
GROUP BY point)

SELECT inc.point, (ISNULL(inc, 0) - ISNULL(out, 0)) as Balans
FROM inc FULL JOIN out ON inc.point = out.point
```

## 60

Посчитать остаток денежных средств **на начало дня 15/04/01** на каждом пункте приема для базы данных с отчетностью
не чаще одного раза в день. Вывод: пункт, остаток.
Замечание. Не учитывать пункты, информации о которых нет до указанной даты.

```sql
SELECT point, (COALESCE(SUM(inc), 0) - COALESCE(SUM(out), 0)) as remain
FROM income_o
FULL JOIN outcome_o USING(point, date)
WHERE date < '15/04/01'
GROUP BY point
```

## 61

Посчитать остаток денежных средств на всех пунктах приема для базы данных с отчетностью не чаще одного раза в день.

```sql
SELECT (COALESCE(SUM(inc), 0) - COALESCE(SUM(out), 0)) as remain
FROM income_o
FULL JOIN outcome_o USING(point, date)
```