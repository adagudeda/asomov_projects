Задание 2. SQL
2.1 Очень усердные ученики.

2.1.1 Условие

Образовательные курсы состоят из различных уроков, каждый из которых состоит из нескольких маленьких заданий. Каждое такое маленькое задание называется "горошиной".
Назовём очень усердным учеником того пользователя, который хотя бы раз за текущий месяц правильно решил 20 горошин за час.

2.1.2 Задача

Дана таблица peas:

Название атрибута|Тип атрибута|Смысловое значение
st_id            |int         |ID ученика
timest           |timestamp   |Время решения карточки
correct          |bool        |Правильно ли решена горошина?
subject          |text        |Дисциплина, в которой находится горошина

Необходимо написать оптимальный запрос, который даст информацию о количестве очень усердных студентов за март 2020 года.
NB! Под усердным студентом мы понимаем студента, который правильно решил 20 задач за текущий месяц.

SELECT
    uniqExact(student_id)
FROM
    (
    SELECT 
          st_id as student_id
        , sum(correct) as correct_answ
    FROM
        default.peas
    WHERE toStartOfMonth(timest) = '2020-03-01'
            AND
          correct = 1
    GROUP BY st_id
    HAVING correct_answ >= 20
    )


2.2 Оптимизация воронки

2.2.1 Условие

Образовательная платформа предлагает пройти студентам курсы по модели trial: студент может решить бесплатно лишь 30 горошин в день. 
Для неограниченного количества заданий в определенной дисциплине студенту необходимо приобрести полный доступ. 
Команда провела эксперимент, где был протестирован новый экран оплаты.

2.2.2 Задача

Дана таблицы: peas (см. выше), studs:

Название атрибута|Тип атрибута|Смысловое значение
st_id            |int         |ID ученика
test_grp         |text        |Метка ученика в данном эксперименте

и final_project_check:

Название атрибута|Тип атрибута|Смысловое значение
st_id            |int         |ID ученика
sale_time        |datetime    |Время покупки
money            |int         |Цена, по которой приобрели данный курс
subject          |text        |Дисциплина, на которую приобрели полный доступ

Необходимо в одном запросе выгрузить следующую информацию о группах пользователей:
* ARPU 
* ARPAU 
* CR в покупку 
* СR активного пользователя в покупку 
* CR пользователя из активности по математике (subject = 'math') в покупку курса по математике


WITH
   -- Отберем активных студентов. Примем, что активный - тот, кто проявлял активность в решении задач
active_studs AS (
   SELECT  st_id
          ,subject
   FROM default.peas
   ),

   -- пропишем активный студент или нет, избавимся от задвоения в таблице studs и к какой группе относится  
students AS (
   SELECT
        l.st_id
       ,l.test_grp
       ,CASE WHEN r.st_id='' THEN 0 ELSE 1 END AS active
   FROM default.studs l
   LEFT JOIN active_studs r USING st_id
   GROUP BY
        l.st_id
       ,r.st_id
       ,l.test_grp
   ),

   -- объединяем чеки и студентов
final AS (
   SELECT *
   FROM students l
   LEFT JOIN default.final_project_check r USING st_id
   )

-- Рассчет метрик  
SELECT
     test_grp
    ,SUM(money) / uniqExact(st_id) as ARPU
    ,sumIf(money, active=1) / countIf(DISTINCT st_id, active=1) as ARPAU
    ,countIf(DISTINCT st_id, money>0) / uniqExact(st_id) as CR
    ,countIf(DISTINCT st_id, active=1) / countIf(DISTINCT st_id, active=1 and money>0) as CR_active
    ,countIf(DISTINCT st_id, active=1 and subject='Math') / countIf(DISTINCT st_id, money>0 and subject='Math') as CR_active_math
FROM final
GROUP BY test_grp