# restaurants

curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field=0&sort_direction=asc&limit=50&offset=0' \
--header 'X-Nickname: Dmitriy' \
--header 'X-Cohort: 10' \
--header 'X-API-KEY: 25c27781-8fde-4b30-a22e-524044a7580f'

По сути, эта таблица не нужна вовсе. Но раз есть, то пусть лежит в stg, места много не занимает. 

# couriers

curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field=0&sort_direction=asc&limit=50&offset=0' \
--header 'X-Nickname: Dmitriy' \
--header 'X-Cohort: 10' \
--header 'X-API-KEY: 25c27781-8fde-4b30-a22e-524044a7580f'

# deliveries

curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field=0&sort_direction=asc&limit=50&offset=0' \
--header 'X-Nickname: Dmitriy' \
--header 'X-Cohort: 10' \
--header 'X-API-KEY: 25c27781-8fde-4b30-a22e-524044a7580f'

# Привет, Ревьюер!

Мне пришлось отказаться от связей в dds слое, потому что в заказах и курьерах данные отличались - в таблице с заказами были курьеры не из таблицы курьеров.
В прочем, это решилось при джойне таблиц.

В остальном - это очень интересный проект, надеюсь на не менее интересные комментарии от тебя :)

Я отошёл от процесса, описанного в задании и выполнял его так, как мне показалось удобнее.
По скольку нам нужно получить статистику за период, я принял решение обновлять данные в витрине в случае нарушения уникальности - сочетания полей courier_id, settlement_year и settlement_month
В остальных таблицах решил, в случае конфликта, не делать ничего - данные о доставках конечны, не нуждаются в обновлении.
