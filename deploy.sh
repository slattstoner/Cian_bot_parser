#!/bin/bash
# Скрипт развёртывания бота на VPS
# Запуск: bash deploy.sh

set -e

BOT_DIR="/opt/bot"
VENV_DIR="$BOT_DIR/venv"

echo "=== Развёртывание Telegram бота ==="

# 1. Остановить текущий сервис (если запущен)
echo "[1/7] Останавливаем текущий сервис..."
systemctl stop bot.service 2>/dev/null || true

# 2. Создать директорию
echo "[2/7] Создаём директорию $BOT_DIR..."
mkdir -p "$BOT_DIR"

# 3. Копируем файлы
echo "[3/7] Копируем файлы..."
cp *.py "$BOT_DIR/"
cp requirements.txt "$BOT_DIR/"
cp .env "$BOT_DIR/" 2>/dev/null || echo "  ВНИМАНИЕ: .env не найден, создайте его вручную!"

# 4. Создать виртуальное окружение
echo "[4/7] Создаём виртуальное окружение..."
python3 -m venv "$VENV_DIR"

# 5. Установить зависимости
echo "[5/7] Устанавливаем зависимости..."
"$VENV_DIR/bin/pip" install --upgrade pip
"$VENV_DIR/bin/pip" install -r "$BOT_DIR/requirements.txt"

# 6. Установить Playwright браузеры
echo "[6/7] Устанавливаем Playwright..."
"$VENV_DIR/bin/playwright" install chromium
"$VENV_DIR/bin/playwright" install-deps chromium

# 7. Настраиваем systemd
echo "[7/7] Настраиваем systemd сервис..."
cp bot.service /etc/systemd/system/bot.service
systemctl daemon-reload
systemctl enable bot.service
systemctl start bot.service

echo ""
echo "=== Готово! ==="
echo "Проверьте статус: systemctl status bot.service"
echo "Логи: journalctl -u bot.service -f"
