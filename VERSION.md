# Project Versions

Current package version: **2.0.0**
Date: **2026-03-11**

## Module versions
- `bot.py` — 2.0.0
- `config.py` — 2.0.0
- `database.py` — 2.0.0
- `handlers.py` — 2.0.0
- `models.py` — 2.0.0
- `parsers.py` — 2.0.0
- `utils.py` — 2.0.0

## Main changes in 2.0.0
- Parser rewritten for more reliable Playwright loading.
- Debug artifacts are saved to `/tmp/bot_parser_debug/`.
- Added version constants in core modules.
- Fixed `choose_plan` import in `bot.py`.
- Fixed missing `balance_text` in `handlers.py` profile view.
- Telegram sending slightly slowed down to reduce flood risk.

## Updating version further
When you change a module:
1. Update the header comment in the file.
2. Update `__version__` in that module.
3. Update this file.
