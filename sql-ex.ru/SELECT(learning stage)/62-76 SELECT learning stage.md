## 62

Посчитать остаток денежных средств на всех пунктах приема **на начало дня 15/04/01** для базы данных с отчетностью не чаще одного раза в день.

```sql
SELECT (COALESCE(SUM(inc), 0) - COALESCE(SUM(out), 0)) as remain
FROM income_o
FULL JOIN outcome_o USING(point, date)
WHERE date < '15/04/01'
```

## 63

Определить имена разных пассажиров, когда-либо летевших на одном и том же месте более одного раза.

```sql
SELECT name
FROM passenger
WHERE id_psg IN (SELECT id_psg
     FROM pass_in_trip
     GROUP BY id_psg, place
     HAVING COUNT(*) > 1)
```

## 64

Используя таблицы `Income` и `Outcome`, для каждого пункта приема определить дни, когда был приход, но не было расхода и наоборот.
Вывод: пункт, дата, тип операции (inc/out), денежная сумма за день.

```sql
WITH inc AS (
SELECT point, date, 'inc' as oper, SUM(inc) as msum
FROM Income
GROUP BY point, date),

out AS (
SELECT point, date, 'out' as oper, SUM(out) as msum
FROM Outcome
GROUP BY point, date)

SELECT point, date, COALESCE(inc.oper, out.oper) as operation, COALESCE(inc.msum, out.msum) as money_sum
FROM inc
FULL JOIN out USING(point, date)
WHERE inc.msum IS NULL OR out.msum is NULL
```

## 65

Пронумеровать уникальные пары `{maker, type}` из `Product`, упорядочив их следующим образом:

- имя производителя (`maker`) по возрастанию;
- тип продукта (`type`) в порядке PC, Laptop, Printer.

Если некий производитель выпускает несколько типов продукции, то выводить его имя только в первой строке;
остальные строки для ЭТОГО производителя должны содержать пустую строку символов ('').

```sql
SELECT row_number() over(order by maker) as numb,
       CASE WHEN num = 1 THEN maker ELSE '' END as maker,
       type
FROM(
     SELECT row_number() over(partition by maker order by maker, ord) 
     as num,
     maker, 
     type
     FROM(
         SELECT DISTINCT maker, type,
    CASE WHEN LOWER(type)='pc' then 1
                WHEN LOWER(type)='laptop' then 2
                ELSE 3 END as ord
         FROM Product) a ) b
```

## 66

Для всех дней в интервале с 01/04/2003 по 07/04/2003 определить число рейсов из Rostov.
Вывод: дата, количество рейсов

```sql
SELECT date, COALESCE(cnt, 0)
FROM generate_series('01/04/2003'::timestamp, '07/04/2003', '1 day') as date
LEFT JOIN (SELECT date, COUNT(DISTINCT trip_no) cnt
  FROM Trip
  JOIN Pass_in_trip USING(trip_no)
  WHERE date between '01/04/2003' and '07/04/2003'
        AND town_from = 'Rostov'
  GROUP BY date) t USING(date)
```

## 67

Найти количество маршрутов, которые обслуживаются наибольшим числом рейсов.
Замечания.

- A - B и B - A считать РАЗНЫМИ маршрутами.
- Использовать только таблицу Trip

```sql
WITH tab as( 
SELECT town_from, town_to, COUNT(DISTINCT trip_no) as cnt
FROM Trip
GROUP BY town_from, town_to)

SELECT COUNT(*)
FROM tab
WHERE cnt = (SELECT MAX(cnt)
       FROM tab)
```

## 68

Найти количество маршрутов, которые обслуживаются наибольшим числом рейсов.
Замечания:

- A - B и B - A считать **ОДНИМ И ТЕМ ЖЕ** маршрутом.
- Использовать только таблицу `Trip`

```sql
WITH tab as( 
SELECT COUNT(DISTINCT trip_no) as cnt
FROM Trip
GROUP BY IIF(town_from > town_to, town_from, town_to), IIF(town_from < town_to, town_from, town_to))

SELECT COUNT(*)
FROM tab
WHERE cnt = (SELECT MAX(cnt)
       FROM tab)
```

## 69

По таблицам `Income` и `Outcome` для каждого пункта приема найти остатки денежных средств на конец каждого дня,
в который выполнялись операции по приходу и/или расходу на данном пункте.
Учесть при этом, что деньги не изымаются, а остатки/задолженность переходят на следующий день.
Вывод: пункт приема, день в формате "dd/mm/yyyy", остатки/задолженность на конец этого дня.

```sql
SELECT DISTINCT point, to_char(date, 'DD/MM/YYYY') AS day,
       SUM(rev) OVER(PARTITION BY point
                     ORDER BY date
                     RANGE UNBOUNDED PRECEDING) as  rem
FROM (
  SELECT point, date, inc as rev
  FROM Income
  UNION ALL
  SELECT  point, date, -out as rev
  FROM Outcome) op
ORDER BY point, day
```

## 70

Укажите сражения, в которых участвовало по меньшей мере три корабля одной и той же страны.

```sql

```

## 71

Найти тех производителей ПК, все модели ПК которых имеются в таблице `PC`.

```sql

```

## 72

Среди тех, кто пользуется услугами только какой-нибудь одной компании, определить имена разных пассажиров, летавших чаще других.
Вывести: имя пассажира и число полетов.

```sql

```

## 73

Для каждой страны определить сражения, в которых не участвовали корабли данной страны.
Вывод: страна, сражение

```sql

```

## 74

Вывести классы всех кораблей России (Russia). Если в базе данных нет классов кораблей России, вывести классы для всех имеющихся в БД стран.
Вывод: страна, класс

```sql  

```

## 75

Для каждого корабля из таблицы `Ships` указать название первого по времени сражения из таблицы Battles,
в котором корабль мог бы участвовать после спуска на воду. 

- Если год спуска на воду неизвестен, взять последнее по времени сражение.
- Если нет сражения, произошедшего после спуска на воду корабля, вывести NULL вместо названия сражения.

Считать, что корабль может участвовать во всех сражениях, которые произошли в год спуска на воду корабля.

Вывод: имя корабля, год спуска на воду, название сражения

Замечание: считать, что не существует двух битв, произошедших в один и тот же день. 

```sql

```

## 76

Определить время, проведенное в полетах, для пассажиров, летавших всегда на разных местах.
Вывод: имя пассажира, время в минутах.

```sql

```