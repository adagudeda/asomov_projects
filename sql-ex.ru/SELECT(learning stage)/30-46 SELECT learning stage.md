## 30

В предположении, что приход и расход денег на каждом пункте приема фиксируется произвольное число раз (первичным ключом в таблицах является столбец code), требуется получить таблицу, в которой каждому пункту за каждую дату выполнения операций будет соответствовать одна строка.
Вывод: point, date, суммарный расход пункта за день (out), суммарный приход пункта за день (inc). Отсутствующие значения считать неопределенными (NULL).

```sql
SELECT point, date, SUM(out), SUM(inc)
FROM(
  SELECT point, date, inc, NULL as out
  FROM Income
  UNION ALL
  SELECT point, date, NULL as inc, out
  FROM Outcome) a
GROUP BY point, date
```
## 31

Для классов кораблей, калибр орудий которых не менее 16 дюймов, укажите класс и страну.

```sql
SELECT class, country
FROM Classes
WHERE bore >= 16
```

## 32

Одной из характеристик корабля является половина куба калибра его главных орудий (`mw`). С точностью до 2 десятичных знаков определите среднее значение `mw` для кораблей каждой страны, у которой есть корабли в базе данных.

```sql
SELECT country,
  CAST(AVG(bore^3 * 0.5) AS NUMERIC(6,2)) as mw
FROM(
  SELECT name, class
  FROM ships
  UNION
  SELECT ship as name, ship as class
  FROM outcomes) a
JOIN Classes USING(class)
GROUP BY country
```

## 33

Укажите корабли, потопленные в сражениях в Северной Атлантике (North Atlantic). Вывод: ship.

```sql
SELECT ship
FROM outcomes
WHERE result = 'sunk' AND battle = 'North Atlantic'
```

## 34

По Вашингтонскому международному договору от начала 1922 г. запрещалось строить линейные корабли водоизмещением более 35 тыс.тонн. Укажите корабли, нарушившие этот договор (учитывать только корабли c известным годом спуска на воду). Вывести названия кораблей.

```sql
SELECT DISTINCT name
FROM ships
JOIN Classes USING(class)
WHERE launched >= 1922
      AND displacement > 35000
      AND type = 'bb'
```

## 35

В таблице `Product` найти модели, которые состоят только из цифр или только из латинских букв (A-Z, без учета регистра).
Вывод: номер модели, тип модели. (Всё это было бы проще, если бы оно умело в регулярные выражения(?)).

```sql
SELECT model, type
FROM Product
WHERE model NOT LIKE '%[^0-9]%' OR model NOT LIKE '%[^a-zA-Z]%' 
```

### More

Как перевести синтакс LIKE на человеческий язык

- LIKE %[0-9]% - a string with digits (string contains digits)
- NOT LIKE %[0-9]% a string without digits (string doesnt contain digits)
- NOT LIKE %[^0-9]% - a string only with digits (string does not contain not digits)
- LIKE %[^0-9]% - string countains not digits

## 36

Перечислите названия головных кораблей, имеющихся в базе данных (учесть корабли в Outcomes).

```sql
SELECT name
FROM ships
WHERE name = class
UNION
SELECT ship
FROM outcomes
WHERE ship IN (SELECT DISTINCT class
    From classes)
```

## 37

Найдите классы, в которые входит только один корабль из базы данных (учесть также корабли в Outcomes).

```sql
SELECT b.class
FROM(
  SELECT name, class
  FROM ships
  UNION
  SELECT DISTINCT ship, ship as class
  FROM outcomes) a
JOIN Classes b ON a.class = b.class
GROUP BY b.class
HAVING count(name) = 1
```

## 38

Найдите страны, имевшие когда-либо классы обычных боевых кораблей ('bb') и имевшие когда-либо классы крейсеров ('bc').

```sql
SELECT country
FROM Classes
WHERE type = 'bb'
INTERSECT
SELECT country
FROM Classes
WHERE type = 'bc'
```

## 39

Найдите корабли, сохранившиеся для будущих сражений; т.е. выведенные из строя в одной битве (damaged), они участвовали в другой, произошедшей позже.

```sql
SELECT DISTINCT t1.ship
FROM(
  (SELECT *
  FROM Outcomes o
  JOIN Battles b ON o.battle = b.name
  WHERE o.result = 'damaged') t1
    JOIN
    (SELECT *
    FROM Outcomes o
    JOIN Battles b ON o.battle = b.name) t2
    ON t1.ship = t2.ship)
WHERE t1.date < t2.date
```

## 40

Найти производителей, которые выпускают более одной модели, при этом все выпускаемые производителем модели являются продуктами одного типа.
Вывести: maker, type

```sql
SELECT maker, type
FROM Product
WHERE maker IN (SELECT maker
    FROM Product
    GROUP BY maker
    HAVING COUNT(DISTINCT type) = 1)
GROUP BY maker, type
HAVING COUNT(model) > 1
```

## 41

Для ПК с максимальным кодом из таблицы PC вывести все его характеристики (кроме кода) в два столбца:

- название характеристики (имя соответствующего столбца в таблице PC);
- значение характеристики

- NOTE решение не принимается, "несовпадение данных" (потому, что начальная длина строк в 10 байт оказалась **недостаточной**)

```sql
WITH all_m AS ( SELECT model, price
  FROM PC
  UNION ALL
  SELECT model, price
  FROM Laptop
  UNION ALL
  SELECT model, price
  FROM Printer)

SELECT maker,
  CASE WHEN MAX(CASE WHEN price IS NULL THEN 1 ELSE 0 END) = 0 THEN
  MAX(price) END
FROM Product
JOIN all_m ON Product.model = all_m.model
GROUP BY maker

```

## 42

Найдите названия кораблей, потопленных в сражениях, и название сражения, в котором они были потоплены.

```sql
SELECT ship, battle
FROM Outcomes
WHERE result = 'sunk'
```

## 43

Укажите сражения, которые произошли в годы, не совпадающие ни с одним из годов спуска кораблей на воду.

```sql
SELECT name
FROM battles
WHERE DATEPART(YEAR, date) NOT IN (SELECT launched
           FROM ships
           WHERE launched IS NOT NULL)
```

## 44

Найдите названия всех кораблей в базе данных, начинающихся с буквы R.

```sql
SELECT name
FROM Ships
WHERE name LIKE 'R%'
UNION
SELECT ship 
FROM Outcomes
WHERE ship LIKE 'R%'
```

## 45

Найдите названия всех кораблей в базе данных, состоящие из трех и более слов (например, King George V).
Считать, что слова в названиях разделяются единичными пробелами, и нет концевых пробелов.

```sql
SELECT name
FROM Ships
WHERE name LIKE '% % %'
UNION
SELECT ship
FROM Outcomes 
WHERE ship LIKE '% % %'
```

## 46

Для каждого корабля, участвовавшего в сражении при Гвадалканале (Guadalcanal), вывести название, водоизмещение и число орудий.

```sql
SELECT ship, displacement, numGuns
FROM Outcomes o 
LEFT JOIN Ships s ON o.ship = s.name 
LEFT JOIN Classes c ON s.class=c.class OR o.ship = c.class
WHERE o.battle = 'Guadalcanal'
```