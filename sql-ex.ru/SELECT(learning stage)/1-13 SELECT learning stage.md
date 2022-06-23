## 1

Найдите номер модели, скорость и размер жесткого диска для всех ПК стоимостью менее 500 дол. Вывести: model, speed и hd

```sql
SELECT model, speed, hd
FROM PC
WHERE price < 500
```

## 2

Найдите производителей принтеров. Вывести: maker

```sql
SELECT DISTINCT maker
FROM Product
WHERE type='Printer'

```

## 3

Найдите номер модели, объем памяти и размеры экранов ПК-блокнотов, цена которых превышает 1000 дол.

```sql
SELECT model, ram, screen
FROM laptop
WHERE price > 1000
```

## 4

Найдите все записи таблицы Printer для цветных принтеров.

```sql
SELECT *
FROM Printer
WHERE color = 'y'
```

## 5

Найдите номер модели, скорость и размер жесткого диска ПК, имеющих 12x или 24x CD и цену менее 600 дол.

```sql
SELECT model, speed, hd
FROM PC
WHERE cd IN('12x', '24x') AND price < 600
```

## 6

Для каждого производителя, выпускающего ПК-блокноты c объёмом жесткого диска не менее 10 Гбайт, найти скорости таких ПК-блокнотов. Вывод: производитель, скорость.

```sql
SELECT DISTINCT a.maker, b.speed
FROM Product as a
JOIN Laptop as b
  ON a.model = b.model
WHERE b.hd >= 10
```

## 7

Найдите номера моделей и цены всех имеющихся в продаже продуктов (любого типа) производителя B (латинская буква).

```sql
SELECT b.model, b.price
FROM Product
JOIN
(SELECT model, price
FROM PC
UNION
SELECT model, price
FROM Laptop
UNION
SELECT model, price
FROM Printer) as b ON Product.model = b.model
WHERE Product.maker = 'B'
```

## 8

Найдите производителя, выпускающего ПК, но не ПК-блокноты.

```sql
SELECT maker
FROM Product
WHERE type = 'PC'
EXCEPT
SELECT maker
FROM Product
WHERE type = 'laptop'
```

## 9

Найдите производителей ПК с процессором не менее 450 Мгц. Вывести: Maker

```sql
SELECT DISTINCT maker
FROM Product as a
JOIN
  (SELECT *
  FROM PC
  WHERE speed >= 400) as b
  ON a.model = b.model
```

## 10

Найдите модели принтеров, имеющих самую высокую цену. Вывести: model, price

```sql
SELECT model, price
FROM Printer
WHERE price IN (
  SELECT MAX(price)
  FROM Printer)
```

## 11

Найдите среднюю скорость ПК.

```sql
SELECT AVG(speed)
FROM PC
```

## 12

Найдите среднюю скорость ПК-блокнотов, цена которых превышает 1000 дол.

```sql
SELECT AVG(speed)
FROM Laptop
WHERE price > 1000
```

## 13

Найдите среднюю скорость ПК, выпущенных производителем A.

```sql
SELECT AVG(speed)
FROM PC
JOIN Product ON PC.model = Product.model
WHERE Product.maker = 'A'
```