
#!/usr/bin/env python3
# bot.py v1.1 (04.03.2026)
# - Увеличено ограничение TELEGRAM_RATE_LIMIT до 30
# - Добавлена обработка новых колбэков для кнопки закрытия тикета

import asyncio
import logging
import signal

from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ConversationHandler
)

from config import TOKEN, DATABASE_URL, TELEGRAM_RATE_LIMIT
from database import Database
from handlers import (
    # Старт и меню
    start, role_chosen, main_menu, help_command, profile,
    # Фильтры
    start_filter, filter_districts, filter_rooms, filter_metros, filter_sources,
    filter_owner, filter_deal_type, filters_done, filter_back,
    toggle_district, toggle_room, metro_line, toggle_metro, metro_clear,
    toggle_source, toggle_owner, toggle_deal_type,
    metro_search_start, handle_metro_search_text, toggle_metro_search,
    # Поддержка
    support_start, handle_support_message,
    # Тикеты
    tickets_list, close_ticket, admin_reply_to_ticket, view_ticket,
    close_ticket_callback,  # новая функция для кнопки
    # Модераторская панель
    mod_panel, mod_panel_back, mod_tickets_callback, mod_closed_tickets_callback, mod_stats_callback,
    # Админ панель (callbacks)
    admin_panel, admin_panel_back,
    admin_stats_callback, admin_users_callback,
    admin_tickets_callback, admin_closed_tickets_callback,
    admin_broadcast_callback, admin_broadcast_mods_callback,
    admin_find_callback, admin_active_subs_callback,
    admin_add_mod_callback, admin_handle_add_mod,
    admin_remove_mod_callback, admin_remove_mod_confirm,
    admin_list_mods_callback,
    admin_debug_callback, admin_debug_toggle,
    admin_balances_callback, admin_banned_callback, admin_export_callback,
    # Админ команды
    activate, grant, stats, users_list, find_user, profile_by_id,
    broadcast, broadcast_mods, test_parse, daily_by_metro,
    users_page, broadcast_confirm,
    add_mod_command, remove_mod_command, mods_list_command,
    debug_on_command, debug_off_command,
    admin_active_subs_command, ban_user, unban_user,
    set_balance, add_balance, export_users,
    # Фоновые задачи
    collector_loop, update_checker_loop,
    # Состояния
    ROLE_SELECTION,
    # ConversationHandler объекты
    SUPPORT_CONV, METRO_SEARCH_CONV, ADD_MOD_CONV
)
from utils import shutdown

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


async def post_init(app: Application):
    await Database.init(DATABASE_URL)
    app.bot_data['telegram_semaphore'] = asyncio.Semaphore(TELEGRAM_RATE_LIMIT)
    app.bot_data['debug_mode'] = False

    tasks = [
        asyncio.create_task(collector_loop(app)),
        asyncio.create_task(update_checker_loop(app))
    ]
    app.bot_data["background_tasks"] = tasks
    logger.info("Бот успешно инициализирован!")


def main():
    if not TOKEN:
        raise ValueError("TOKEN не установлен! Проверьте .env файл.")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL не установлен! Проверьте .env файл.")

    app = Application.builder().token(TOKEN).post_init(post_init).build()

    # ===== ConversationHandler для выбора роли =====
    role_conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={ROLE_SELECTION: [CallbackQueryHandler(role_chosen, pattern='^role_')]},
        fallbacks=[],
        allow_reentry=True
    )
    app.add_handler(role_conv)

    # ===== ConversationHandler для поддержки =====
    app.add_handler(SUPPORT_CONV)

    # ===== ConversationHandler для поиска метро =====
    app.add_handler(METRO_SEARCH_CONV)

    # ===== ConversationHandler для добавления модератора =====
    app.add_handler(ADD_MOD_CONV)

    # ===== Команды =====
    app.add_handler(CommandHandler('menu', main_menu))
    app.add_handler(CommandHandler('admin', admin_panel))
    app.add_handler(CommandHandler('mod', mod_panel))
    app.add_handler(CommandHandler('help', help_command))
    app.add_handler(CommandHandler('active_subs', admin_active_subs_command))
    app.add_handler(CommandHandler('add_mod', add_mod_command))
    app.add_handler(CommandHandler('remove_mod', remove_mod_command))
    app.add_handler(CommandHandler('mods', mods_list_command))
    app.add_handler(CommandHandler('debug_on', debug_on_command))
    app.add_handler(CommandHandler('debug_off', debug_off_command))
    app.add_handler(CommandHandler('act', activate))
    app.add_handler(CommandHandler('grant', grant))
    app.add_handler(CommandHandler('stats', stats))
    app.add_handler(CommandHandler('users', users_list))
    app.add_handler(CommandHandler('find', find_user))
    app.add_handler(CommandHandler('profile', profile_by_id))
    app.add_handler(CommandHandler('tickets', tickets_list))
    app.add_handler(CommandHandler('close_ticket', close_ticket))
    app.add_handler(CommandHandler('reply', admin_reply_to_ticket))
    app.add_handler(CommandHandler('view_ticket', view_ticket))
    app.add_handler(CommandHandler('broadcast', broadcast))
    app.add_handler(CommandHandler('broadcast_mods', broadcast_mods))
    app.add_handler(CommandHandler('testparse', test_parse))
    app.add_handler(CommandHandler('daily', daily_by_metro))
    app.add_handler(CommandHandler('ban', ban_user))
    app.add_handler(CommandHandler('unban', unban_user))
    app.add_handler(CommandHandler('set_balance', set_balance))
    app.add_handler(CommandHandler('add_balance', add_balance))
    app.add_handler(CommandHandler('export_users', export_users))

    # ===== Callback кнопки — общие =====
    app.add_handler(CallbackQueryHandler(main_menu, pattern='^main_menu$'))
    app.add_handler(CallbackQueryHandler(profile, pattern='^profile$'))
    app.add_handler(CallbackQueryHandler(help_command, pattern='^help$'))
    app.add_handler(CallbackQueryHandler(choose_plan, pattern='^cp$'))
    app.add_handler(CallbackQueryHandler(support_start, pattern='^support$'))

    # ===== Подписка =====
    app.add_handler(CallbackQueryHandler(plan_chosen, pattern='^p\\d+m$'))

    # ===== Фильтры =====
    app.add_handler(CallbackQueryHandler(start_filter, pattern='^fl$'))
    app.add_handler(CallbackQueryHandler(filter_districts, pattern='^f_districts$'))
    app.add_handler(CallbackQueryHandler(filter_rooms, pattern='^f_rooms$'))
    app.add_handler(CallbackQueryHandler(filter_metros, pattern='^f_metros$'))
    app.add_handler(CallbackQueryHandler(filter_sources, pattern='^f_sources$'))
    app.add_handler(CallbackQueryHandler(filter_owner, pattern='^f_owner$'))
    app.add_handler(CallbackQueryHandler(filter_deal_type, pattern='^f_deal_type$'))
    app.add_handler(CallbackQueryHandler(filters_done, pattern='^f_done$'))
    app.add_handler(CallbackQueryHandler(filter_back, pattern='^f_back$'))
    app.add_handler(CallbackQueryHandler(toggle_district, pattern='^d_.+$'))
    app.add_handler(CallbackQueryHandler(toggle_room, pattern='^r_.+$'))
    app.add_handler(CallbackQueryHandler(metro_line, pattern='^l_.+$'))
    app.add_handler(CallbackQueryHandler(toggle_metro, pattern='^m_[A-Za-z0-9]+_\\d+$'))
    app.add_handler(CallbackQueryHandler(metro_clear, pattern='^metro_clear$'))
    app.add_handler(CallbackQueryHandler(toggle_source, pattern='^src_'))
    app.add_handler(CallbackQueryHandler(toggle_owner, pattern='^owner_'))
    app.add_handler(CallbackQueryHandler(toggle_deal_type, pattern='^deal_'))
    app.add_handler(CallbackQueryHandler(toggle_metro_search, pattern='^ms_\\d+$'))

    # ===== Платежи =====

    # ===== Модераторские callbacks =====
    app.add_handler(CallbackQueryHandler(mod_panel_back, pattern='^mod_panel_back$'))
    app.add_handler(CallbackQueryHandler(mod_tickets_callback, pattern='^mod_tickets$'))
    app.add_handler(CallbackQueryHandler(mod_closed_tickets_callback, pattern='^mod_closed_tickets$'))
    app.add_handler(CallbackQueryHandler(mod_stats_callback, pattern='^mod_stats$'))
    app.add_handler(CallbackQueryHandler(close_ticket_callback, pattern='^close_ticket_'))  # кнопка закрытия

    # ===== Админские callbacks =====
    app.add_handler(CallbackQueryHandler(admin_panel_back, pattern='^admin_panel_back$'))
    app.add_handler(CallbackQueryHandler(admin_stats_callback, pattern='^admin_stats$'))
    app.add_handler(CallbackQueryHandler(admin_users_callback, pattern='^admin_users_'))
    app.add_handler(CallbackQueryHandler(admin_tickets_callback, pattern='^admin_tickets$'))
    app.add_handler(CallbackQueryHandler(admin_closed_tickets_callback, pattern='^admin_closed_tickets$'))
    app.add_handler(CallbackQueryHandler(admin_broadcast_callback, pattern='^admin_broadcast$'))
    app.add_handler(CallbackQueryHandler(admin_broadcast_mods_callback, pattern='^admin_broadcast_mods$'))
    app.add_handler(CallbackQueryHandler(admin_find_callback, pattern='^admin_find$'))
    app.add_handler(CallbackQueryHandler(admin_active_subs_callback, pattern='^admin_active_subs$'))
    app.add_handler(CallbackQueryHandler(admin_remove_mod_callback, pattern='^admin_remove_mod$'))
    app.add_handler(CallbackQueryHandler(admin_remove_mod_confirm, pattern='^rmmod_'))
    app.add_handler(CallbackQueryHandler(admin_list_mods_callback, pattern='^admin_list_mods$'))
    app.add_handler(CallbackQueryHandler(admin_debug_callback, pattern='^admin_debug$'))
    app.add_handler(CallbackQueryHandler(admin_debug_toggle, pattern='^dbg_'))
    app.add_handler(CallbackQueryHandler(admin_balances_callback, pattern='^admin_balances$'))
    app.add_handler(CallbackQueryHandler(admin_banned_callback, pattern='^admin_banned$'))
    app.add_handler(CallbackQueryHandler(admin_export_callback, pattern='^admin_export$'))

    # ===== Пагинация и рассылка =====
    app.add_handler(CallbackQueryHandler(users_page, pattern='^users_page_'))
    app.add_handler(CallbackQueryHandler(broadcast_confirm, pattern='^bc_'))

    # ===== Graceful shutdown =====
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(app)))

    logger.info("🚀 Бот запускается...")
    app.run_polling(allowed_updates=['message', 'callback_query'])


if __name__ == '__main__':
    main()