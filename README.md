# test-task-for-Aero
https://taniadi.notion.site/Aero-DE-c7b2b267caa14570935d7cba30218865


Для выполнения задания установила postgres 15.4
Версия python - 3.10

Для создания таблиц нужно:
1. заполнить файл settings.yaml
2. запустить файл table_initialization.py

Для оркестрации использовала crontab:

`% crontab -e`

`00 */12 * * * source /Users/malinamedusa/PycharmProjects/test-task-for-Aero/lovenv/bin/activate && cd /Users/malinamedusa/PycharmProjects/test-task-for-Aero/ && python3 load_data_in_table.py`

`esc :wq`

По итогу выполнения в папке log появляется подробная информация о пройденных процессах

Был ещё план подключить тг бота чтобы смотреть логи либо отправлять на почту письмо с информацией о запуске, но долго провозилась с ошибками связанными с M1:

`...
ImportError: dlopen(/Users/malinamedusa/PycharmProjects/test-task-for-Aero/lovenv/lib/python3.12/site-packages/psycopg2/_psycopg.cpython-312-darwin.so, 0x0002): tried: '/Users/malinamedusa/PycharmProjects/test-task-for-Aero/lovenv/lib/python3.12/site-packages/psycopg2/_psycopg.cpython-312-darwin.so' (mach-o file, but is an incompatible architecture (have 'x86_64', need 'arm64')), '/System/Volumes/Preboot/Cryptexes/OS/Users/malinamedusa/PycharmProjects/test-task-for-Aero/lovenv/lib/python3.12/site-packages/psycopg2/_psycopg.cpython-312-darwin.so' (no such file), '/Users/malinamedusa/PycharmProjects/test-task-for-Aero/lovenv/lib/python3.12/site-packages/psycopg2/_psycopg.cpython-312-darwin.so' (mach-o file, but is an incompatible architecture (have 'x86_64', need 'arm64'))`

Хотя активен сейчас как раз arm64

`(lovenv) malinamedusa@MacBook-Pro test-task-for-Aero % arch`                 
`arm64`

Если вы вдруг сталкивались с такой проблемой - подскажите как решили)

