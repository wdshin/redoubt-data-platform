import logging
import os

import psycopg2
import psycopg2.extras
import secrets
from telegram import ForceReply, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)
"""
DB Schema:
CREATE TABLE IF NOT EXISTS api_keys (
            id bigserial NOT NULL primary key,
            create_time timestamp with time zone NOT NULL,
            api_key varchar,
            user_id varchar,
            username varchar,
            title varchar,
            is_bot bool,
            language_code varchar
);

create unique index if not exists api_keys_idx1 on api_keys(api_key);
create unique index if not exists api_keys_idx2 on api_keys(user_id);
"""

def generate_key() -> str:
    return secrets.token_hex(32)

async def get_key(user, refresh=False) -> str:
    conn = psycopg2.connect()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute("select api_key from api_keys where user_id = %s", (str(user.id),))
        res = cursor.fetchone()
        if refresh:
            cursor.execute("delete from api_keys where user_id = %s", (str(user.id),))
            conn.commit()
            res = None
        if res is None:
            key = generate_key()
            title = " ".join(list(filter(lambda x: x is not None, [user.first_name, user.last_name])))
            cursor.execute("""
            insert into api_keys(create_time, api_key, user_id, username, title, is_bot, language_code)
            values (now(), %s, %s, %s, %s, %s, %s)
            """, (key, str(user.id), user.username, title, user.is_bot, user.language_code))
            conn.commit()
            return key
        logging.info(res)
        return res['api_key']
    finally:
        cursor.close()
        conn.close()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    logging.info(f"/start from {user}")
    key = await get_key(user, refresh=False)

    await update.message.reply_html(f"Hi {user.mention_html()}! Here is your API key: <pre>{key}</pre>")

async def refresh(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    logging.info(f"/refresh from {user}")
    key = await get_key(user, refresh=True)

    await update.message.reply_html(f"Hi {user.mention_html()}! Here is your updated API key: <pre>{key}</pre>")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("""Supported actions:
    * Send /start to get your API key
    * Send /refresh to refresh your API key
    """)


def main() -> None:
    """Start the bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(os.environ.get('API_TOKEN')).build()

    # on different commands - answer in Telegram
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("refresh", refresh))
    application.add_handler(CommandHandler("help", help_command))

    # Run the bot until the user presses Ctrl-C
    application.run_polling()

if __name__ == "__main__":
    main()