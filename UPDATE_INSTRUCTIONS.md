# Обновление бота

## Что изменено

- убраны кнопки и маршруты встроенной оплаты из `bot.py`
- главное меню упрощено: фильтры, профиль, поддержка, помощь
- включён режим свободного доступа через `FREE_ACCESS_MODE=1`
- подбор объявлений стал строгим по метро и округу:
  - если пользователь выбрал округ, объявление без определённого округа не отправляется
  - если пользователь выбрал станции метро, объявление должно содержать одну из них
- поиск метро допускает ручной ввод станции, если её нет в списке
- парсер переписан:
  - реальные `wait_for_selector`
  - сохранение debug HTML в `/tmp/*_debug.html`
  - логирование статуса, final URL, title, HTML length
  - реальное подключение proxy из `PROXY_URL`
  - последовательный запуск источников вместо 4 одновременных Chromium
- рассылка стала безопаснее для Telegram:
  - мягкий rate limit через `Semaphore`
  - `send_photo` вместо `send_media_group`
  - обработка `RetryAfter`, `TimedOut`, `Forbidden`

## Перед выкладкой обязательно

Секреты из старого контекста считаются скомпрометированными. Перед запуском:

1. перевыпусти `TOKEN` бота в BotFather
2. смени пароль PostgreSQL
3. выпусти новый `DADATA_API_KEY`
4. выпусти новый `PAYMENT_PROVIDER_TOKEN`, когда вернёшь оплату
5. выпусти новый `TONCENTER_API_KEY`, когда вернёшь TON-платежи

## Что добавить в `.env`

```env
TOKEN=NEW_TELEGRAM_TOKEN
ADMIN_ID=YOUR_ADMIN_ID
DATABASE_URL=postgresql://bot_user:NEW_PASSWORD@localhost:5432/bot_db
DADATA_API_KEY=NEW_DADATA_KEY
TON_WALLET=
TONCENTER_API_KEY=
PAYMENT_PROVIDER_TOKEN=
PROXY_URL=
GITHUB_TOKEN=
FREE_ACCESS_MODE=1
```

## Как залить обновление в GitHub

На локальной машине:

```bash
git checkout -b fix/parser-filters-ui
cp -r Cian_bot_parser-main/* /path/to/your/repo/
cd /path/to/your/repo
git add .
git commit -m "Refactor parser, strict filters, disable payments"
git push origin fix/parser-filters-ui
```

Если работаешь прямо на сервере:

```bash
cd /opt/bot
git checkout -b fix/parser-filters-ui
# скопируй обновлённые файлы в /opt/bot
python -m py_compile bot.py handlers.py database.py parsers.py config.py utils.py models.py
git add .
git commit -m "Refactor parser, strict filters, disable payments"
git push origin fix/parser-filters-ui
```

## Как обновить сервер после merge

```bash
cd /opt/bot
git pull origin main
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
sudo systemctl restart bot.service
sudo systemctl status bot.service
journalctl -u bot.service -n 200 --no-pager
```

## Что проверить после перезапуска

```bash
journalctl -u bot.service -f
```

И отдельно:

```bash
ls /tmp/*_debug.html
```

Нужно увидеть файлы вида:

- `/tmp/cian_sale_debug.html`
- `/tmp/cian_rent_debug.html`
- `/tmp/avito_sale_debug.html`
- `/tmp/avito_rent_debug.html`

## Быстрая проверка логики

1. зайти в `/start`
2. выбрать роль
3. настроить фильтр только по одной станции метро
4. настроить фильтр только по одному округу
5. настроить фильтр одновременно по станции и округу
6. выполнить `/testparse`
7. проверить, что в логах есть найденные карточки
8. проверить, что отправляются только подходящие объявления

## Если ЦИАН или Авито снова дают 0 карточек

Смотри:

```bash
head -200 /tmp/cian_sale_debug.html
head -200 /tmp/avito_sale_debug.html
```

Если там капча или антибот:

- включай рабочий `PROXY_URL`
- уменьши частоту парсинга
- проверь доступность сайта с VPS

## Возврат оплаты позже

Когда будешь возвращать оплату:

1. верни callback handlers в `bot.py`
2. верни кнопку оплаты в `main_menu()`
3. заново проверь `successful_payment`, `pay_command`, TON verify
4. установи новые ключи в `.env`
