# Малый e2e: Portal / Inbounds

Все три DAG и загрузки в PostgreSQL, MongoDB и Garage завершились успешно. Полная выгрузка не запускалась.

HTTP-попыток: 14 из 30.

## games_metadata — portal-e2e-001-games_metadata
Пакет: `4c35f573910e71e1e26d6d5274f3e05c032b95b140a865d3a2d915e91db7f786`.
Хранилища: PostgreSQL, MongoDB и Garage — одинаковая проверенная контрольная сумма `9317f3305c8844ce99fb46ca31173617a2e67b3ab74e585425314aae3381516d` для observations; все наборы сверены при чтении.

## runs — portal-e2e-001-runs
Пакет: `92f53b8aa7e7fe356295101b2d2775877e75ed6b37e94172543b298ed9c4fc88`.
Хранилища: PostgreSQL, MongoDB и Garage — одинаковая проверенная контрольная сумма `3dc7691c70dc2dcbccc53e04e460c79365e6f2244ccc65f3106f6ccc98a3c9b1` для observations; все наборы сверены при чтении.
- new: 9 записей, 1/1 страниц, coverage `verified_public_partitions`.
- verified: 200 записей, 2/44 страниц, coverage `partial`.
- rejected: 200 записей, 2/8 страниц, coverage `partial`.
- WR history: 1 страница, coverage `partial`.

## players — portal-e2e-001-players
Пакет: `3efc03aaf687488bfdc2791861779b62bcf60d98bcdd7fffa023bcacd8d56db2`.
Хранилища: PostgreSQL, MongoDB и Garage — одинаковая проверенная контрольная сумма `3cb74d7a60a6931ee80fde5e9a56d2d83a1736be76032483168d0459f5bc80b4` для observations; все наборы сверены при чтении.
Профильные observations: 1125; network responses в этом пакете нет — используются исходные наблюдения runs.

Следующий этап — просмотр этого результата пользователем. Полный сбор остаётся на паузе.

## Полный Half-Life — `half-life-full-001`

Все три DAG и три ODS-загрузки завершились `success`/`ready`. Бюджет: 73/300
HTTP-попыток. Метаданные: игра `n268nk6p`, 4 full-game категории. Runs: 4 243
уникальных рана, 5 244 payload, 72 responses, 34 484 observations. Все страницы
12 разделов category × status получены, граничные страницы проверены. Счётчики
по категориям: WON 4 078; Steam 130; PS2 8; WON Third Person 27. Все эти
разделы имеют `verified_public_partitions`. WR history: по одной странице на
категорию, `partial`; итоговый runs snapshot также `partial`.

Players использовал встроенные публичные профили из runs: 941 участник и
доступный профиль, 941 payload, 29 662 observations, 0 новых HTTP responses.
Coverage `verified_public_partitions`.

| Snapshot | payloads | responses | observations | digest при первичной и повторной загрузке |
|---|---:|---:|---:|---|
| games_metadata | 33 | 1 | 32 | `f605a902f1ec66706f1cc9d02552e063a4fc302c9e0f25ed4a79771ed4d74c9e` |
| runs | 5 244 | 72 | 34 484 | `50c9e9dfad7c927fdc02b8bdfd109ccb1e8c37a7e281e42369f288a6359ab413` |
| players | 941 | 0 | 29 662 | `5775372c7bf61a1a324240e4362d2745c24868f9c5adc4c1ccec528620017acd` |

Для каждого snapshot digest сверялся в PostgreSQL, MongoDB и Garage. Затем те же
три локальных пакета повторно загружены в каждый сервис: все девять итоговых
digest совпали, HTTP-бюджет не изменился. Это проверка идемпотентности replay
готового пакета, а не обещание exactly-once для повторного запроса API.
