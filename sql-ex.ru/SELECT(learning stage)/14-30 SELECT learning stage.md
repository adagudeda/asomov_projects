## 14

Найдите класс, имя и страну для кораблей из таблицы Ships, имеющих не менее 10 орудий.

```sql
SELECT Ships.class, name, country
FROM Ships
JOIN Classes ON Classes.class = Ships.class
WHERE numGuns >= 10
```

## 15

Найдите размеры жестких дисков, совпадающих у двух и более PC. Вывести: HD

```sql
SELECT hd
FROM PC
GROUP BY hd
HAVING COUNT(code) >= 2
```

## 16

Найдите пары моделей PC, имеющих одинаковые скорость и RAM. В результате каждая пара указывается только один раз, т.е. (i,j), но не (j,i), Порядок вывода: модель с большим номером, модель с меньшим номером, скорость и RAM.

```sql
SELECT DISTINCT a.model, b.model, a.speed, a.ram
FROM PC a, PC b
WHERE a.speed = b.speed and a.ram = b.ram and a.model > b.model
```

## 17

Найдите модели ПК-блокнотов, скорость которых меньше скорости каждого из ПК.
Вывести: type, model, speed

```sql
SELECT DISTINCT a.type, b.model, b.speed
FROM Product a, Laptop b
WHERE a.model = b.model
  AND b.speed < ALL(SELECT speed
        FROM PC)
```

## 18

Найдите производителей самых дешевых цветных принтеров. Вывести: maker, price

```sql
SELECT DISTINCT maker, price
FROM Product a, Printer b
WHERE a.model = b.model 
  AND b.color = 'y'
  AND b.price = (SELECT MIN(price)
      FROM Printer
      WHERE color = 'y')
```

## 19

Для каждого производителя, имеющего модели в таблице Laptop, найдите средний размер экрана выпускаемых им ПК-блокнотов.
Вывести: maker, средний размер экрана.

```sql
SELECT maker, AVG(screen)
FROM Product a, Laptop b
WHERE a.model = b.model
GROUP BY maker
```

## 20

Найдите производителей, выпускающих по меньшей мере три различных модели ПК. Вывести: Maker, число моделей ПК.

```sql
SELECT maker, COUNT(model) as number
FROM Product
WHERE type = 'PC'
GROUP BY maker
HAVING COUNT(model) >= 3
```

## 21

Найдите максимальную цену ПК, выпускаемых каждым производителем, у которого есть модели в таблице PC.
Вывести: maker, максимальная цена.

```sql
SELECT maker, MAX(price)
FROM Product a, PC b
WHERE a.model = b.model
GROUP BY maker
```

## 22

Для каждого значения скорости ПК, превышающего 600 МГц, определите среднюю цену ПК с такой же скоростью. Вывести: speed, средняя цена.

```sql
SELECT speed, AVG(price)
FROM PC
WHERE speed > 600
GROUP BY speed
```

## 23

Найдите производителей, которые производили бы как ПК
со скоростью не менее 750 МГц, так и ПК-блокноты со скоростью не менее 750 МГц.
Вывести: Maker

```sql
SELECT DISTINCT maker
FROM Product a, Laptop b
WHERE a.model = b.model 
  AND b.speed >= 750
INTERSECT
SELECT DISTINCT maker
FROM Product a, PC b
WHERE a.model = b.model  
  AND b.speed >= 750
```

## 24

Перечислите номера моделей любых типов, имеющих самую высокую цену по всей имеющейся в базе данных продукции.

```sql
WITH all_models AS (
SELECT price, model
FROM PC
UNION
SELECT price, model
FROM Laptop
UNION
SELECT price, model
FROM Printer)

SELECT model
FROM all_models
WHERE price = (SELECT MAX(price)
    FROM all_models)
```

## 25

Найдите производителей принтеров, которые производят ПК с наименьшим объемом RAM и с самым быстрым процессором среди всех ПК, имеющих наименьший объем RAM. Вывести: Maker

```sql
SELECT DISTINCT maker
FROM Product
WHERE type = 'Printer'
  AND maker IN (
SELECT maker
FROM Product
WHERE model IN(
  SELECT model 
  FROM PC
  WHERE ram = (SELECT MIN(RAM)
      FROM PC)
      AND speed = (SELECT MAX(speed)
          FROM PC
          WHERE ram = (SELECT MIN(RAM) FROM PC))))
```

## 26

Найдите среднюю цену ПК и ПК-блокнотов, выпущенных производителем A (латинская буква). Вывести: одна общая средняя цена.

```sql
SELECT AVG(price)
FROM (
  SELECT maker, price
  FROM Product a
  JOIN(
    SELECT price, model
    FROM PC
    UNION ALL
    SELECT price, model
    FROM Laptop) b ON a.model = b.model
  WHERE maker = 'A') as f
```

## 27

Найдите средний размер диска ПК каждого из тех производителей, которые выпускают и принтеры. Вывести: maker, средний размер HD.

```sql
SELECT maker, AVG(hd)
FROM (
  SELECT maker, hd
  FROM Product a
  JOIN (
    SELECT model, hd
    FROM PC) b ON a.model = b.model) c
WHERE maker IN (SELECT DISTINCT maker
    FROM Product
    WHERE type = 'Printer')
GROUP BY maker
```

## 28

Используя таблицу `Product`, определить количество производителей, выпускающих по одной модели.

```sql
SELECT COUNT(maker)
FROM(
  SELECT maker, COUNT(model) as number
  FROM Product
  GROUP BY maker
  HAVING COUNT(model) = 1) a
```

## 29

В предположении, что приход и расход денег на каждом пункте приема фиксируется не чаще одного раза в день [т.е. первичный ключ (пункт, дата)], написать запрос с выходными данными (пункт, дата, приход, расход). Использовать таблицы `Income_o` и `Outcome_o`.

```sql
SELECT point, date, MIN(inc), MIN(out)
FROM(
  SELECT point, date, inc, NULL as out
  FROM Income_o
  UNION ALL
  SELECT point, date, NULL as inc, out
  FROM Outcome_o) a
GROUP BY point, date
```

## 30

В предположении, что приход и расход денег на каждом пункте приема фиксируется произвольное число раз (первичным ключом в таблицах является столбец code), требуется получить таблицу, в которой каждому пункту за каждую дату выполнения операций будет соответствовать одна строка.
Вывод: `point`, `date`, суммарный расход пункта за день (`out`), суммарный приход пункта за день (`inc`). Отсутствующие значения считать неопределенными (`NULL`).

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
