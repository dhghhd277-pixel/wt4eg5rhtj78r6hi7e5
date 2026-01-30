Краткая инструкция по запуску бота

1) Скопируйте `.env.example` в `.env` и укажите ваш токен:

```powershell
copy .env.example .env
# затем отредактируйте .env в любом редакторе и вставьте токен
```

2) Создайте виртуальное окружение и установите зависимости:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

3) Запустите бота:

```powershell
python bot.py
```

Файлы и структура:
- `bot.py` — основной скрипт бота
- `requirements.txt` — зависимости
- `.env.example` — пример переменных окружения
- `data/` — автосоздаваемая папка для `categories.json` и `products.json`

Если нужно — могу добавить обработку логов, Dockerfile или CI-конфиг.