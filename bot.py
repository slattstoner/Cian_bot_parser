#!/usr/bin/env python3
import asyncio
import logging
import signal
import json
import time
import random
import re
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, PreCheckoutQueryHandler, ConversationHandler

from config import TOKEN, PAYMENT_PROVIDER_TOKEN, DATABASE_URL
from database import Database
from handlers import (
    start, role_chosen, main_menu, help_command, profile,
    choose_plan, plan_chosen, pay_stars, pay_ton, pay_rub,
    pre_checkout, successful_payment, pay_command,
    start_filter, filter_districts, filter_rooms, filter_metros, filter_sources,
    filter_owner, filter_deal_type, filters_done, filter_back,
    toggle_district, toggle_room, metro_line, toggle_metro,
    toggle_source, toggle_owner, toggle_deal_type,
    metro_search_start, handle_metro_search_text,
    support_start, handle_support_message,
    tickets_list, close_ticket, admin_reply_to_ticket,
    mod_panel, mod_tickets_callback, mod_stats_callback, mod_panel_back,
    admin_panel, admin_panel_back, admin_stats_callback, admin_users_callback,
    admin_tickets_callback, admin_broadcast_callback, admin_find_callback,
    admin_active_subs_callback, admin_add_mod_callback, admin_handle_add_mod,
    admin_remove_mod_callback, admin_remove_mod_confirm, admin_list_mods_callback,
    admin_debug_callback, admin_debug_toggle, admin_balances_callback,
    activate, grant, stats, users_list, find_user, profile_by_id,
    broadcast, test_parse, daily_by_metro, users_page, broadcast_confirm,
    add_mod_command, remove_mod_command, mods_list_command, debug_on_command, debug_off_command,
    collector_loop, update_checker_loop, ROLE_SELECTION
)
from utils import shutdown

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Глобальный семафор для ограничения скорости отправки сообщений
telegram_semaphore = asyncio.Semaphore(20)  # TELEGRAM_RATE_LIMIT

async def post_init(app: Application):
    await Database.init(DATABASE_URL)
    # Запуск фоновых задач
    asyncio.create_task(collector_loop(app))
    asyncio.create_task(update_checker_loop(app))
    app.bot_data['debug_mode'] = False
    logger.info("Бот успешно инициализирован и запущен")

def main():
    app = Application.builder().token(TOKEN).post_init(post_init).build()

    # ConversationHandler для выбора роли
    role_conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={ROLE_SELECTION: [CallbackQueryHandler(role_chosen, pattern='^role_')]},
        fallbacks=[]
    )
    app.add_handler(role_conv)

    # Команды
    app.add_handler(CommandHandler('menu', main_menu))
    app.add_handler(CommandHandler('admin', admin_panel))
    app.add_handler(CommandHandler('mod', mod_panel))
    app.add_handler(CommandHandler('active_subs', admin_active_subs_callback))
    app.add_handler(CommandHandler('add_mod', add_mod_command))
    app.add_handler(CommandHandler('remove_mod', remove_mod_command))
    app.add_handler(CommandHandler('mods', mods_list_command))
    app.add_handler(CommandHandler('debug_on', debug_on_command))
    app.add_handler(CommandHandler('debug_off', debug_off_command))
    app.add_handler(CommandHandler('pay', pay_command))
    app.add_handler(CommandHandler('act', activate))
    app.add_handler(CommandHandler('grant', grant))
    app.add_handler(CommandHandler('stats', stats))
    app.add_handler(CommandHandler('users', users_list))
    app.add_handler(CommandHandler('find', find_user))
    app.add_handler(CommandHandler('profile', profile_by_id))
    app.add_handler(CommandHandler('tickets', tickets_list))
    app.add_handler(CommandHandler('close_ticket', close_ticket))
    app.add_handler(CommandHandler('reply', admin_reply_to_ticket))
    app.add_handler(CommandHandler('broadcast', broadcast))
    app.add_handler(CommandHandler('testparse', test_parse))
    app.add_handler(CommandHandler('daily', daily_by_metro))

    # Callback-кнопки
    app.add_handler(CallbackQueryHandler(main_menu, pattern='^main_menu$'))
    app.add_handler(CallbackQueryHandler(profile, pattern='^profile$'))
    app.add_handler(CallbackQueryHandler(help_command, pattern='^help$'))
    app.add_handler(CallbackQueryHandler(support_start, pattern='^support$'))
    app.add_handler(CallbackQueryHandler(choose_plan, pattern='^cp$'))
    app.add_handler(CallbackQueryHandler(plan_chosen, pattern='^p\\d+m$'))
    app.add_handler(CallbackQueryHandler(pay_stars, pattern='^pay_stars$'))
    app.add_handler(CallbackQueryHandler(pay_ton, pattern='^pay_ton$'))
    app.add_handler(CallbackQueryHandler(pay_rub, pattern='^pay_rub$'))
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
    app.add_handler(CallbackQueryHandler(toggle_metro, pattern='^m_.+$'))
    app.add_handler(CallbackQueryHandler(toggle_source, pattern='^src_'))
    app.add_handler(CallbackQueryHandler(toggle_owner, pattern='^owner_'))
    app.add_handler(CallbackQueryHandler(toggle_deal_type, pattern='^deal_'))
    app.add_handler(CallbackQueryHandler(metro_search_start, pattern='^metro_search$'))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_metro_search_text))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_support_message))

    if PAYMENT_PROVIDER_TOKEN:
        app.add_handler(PreCheckoutQueryHandler(pre_checkout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))

    # Админские callback'и
    app.add_handler(CallbackQueryHandler(admin_panel_back, pattern='^admin_panel_back$'))
    app.add_handler(CallbackQueryHandler(admin_stats_callback, pattern='^admin_stats$'))
    app.add_handler(CallbackQueryHandler(admin_users_callback, pattern='^admin_users_'))
    app.add_handler(CallbackQueryHandler(admin_tickets_callback, pattern='^admin_tickets$'))
    app.add_handler(CallbackQueryHandler(admin_broadcast_callback, pattern='^admin_broadcast$'))
    app.add_handler(CallbackQueryHandler(admin_find_callback, pattern='^admin_find$'))
    app.add_handler(CallbackQueryHandler(admin_active_subs_callback, pattern='^admin_active_subs$'))
    app.add_handler(CallbackQueryHandler(admin_add_mod_callback, pattern='^admin_add_mod$'))
    app.add_handler(CallbackQueryHandler(admin_remove_mod_callback, pattern='^admin_remove_mod$'))
    app.add_handler(CallbackQueryHandler(admin_remove_mod_confirm, pattern='^remove_mod_'))
    app.add_handler(CallbackQueryHandler(admin_list_mods_callback, pattern='^admin_list_mods$'))
    app.add_handler(CallbackQueryHandler(admin_debug_callback, pattern='^admin_debug$'))
    app.add_handler(CallbackQueryHandler(admin_debug_toggle, pattern='^debug_'))
    app.add_handler(CallbackQueryHandler(admin_balances_callback, pattern='^admin_balances$'))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_handle_add_mod))

    # Модераторские callback'и
    app.add_handler(CallbackQueryHandler(mod_panel_back, pattern='^mod_panel_back$'))
    app.add_handler(CallbackQueryHandler(mod_tickets_callback, pattern='^mod_tickets$'))
    app.add_handler(CallbackQueryHandler(mod_stats_callback, pattern='^mod_stats$'))

    # Пагинация пользователей и подтверждение рассылки
    app.add_handler(CallbackQueryHandler(users_page, pattern='^users_page_'))
    app.add_handler(CallbackQueryHandler(broadcast_confirm, pattern='^bc_'))

    # Обработка сигналов для graceful shutdown
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(app)))

    logger.info("Бот запускается...")
    app.run_polling()

if __name__ == '__main__':
    main()