from datetime import timedelta

# Importa o Hook do provedor oficial do Airflow para Telegram
from airflow.providers.telegram.hooks.telegram import TelegramHook

TELEGRAM_CONN_ID = "telegram_default"
TELEGRAM_CHAT_ID = "XXXXXXXXX"  # Updated


# Formata a mensagem HTML e envia usando o Hook do Telegram
def _send_telegram(context, status):
    ti = context.get("task_instance")

    if status == "success":
        emoji, title = "🟢", f"Success: {ti.task_id}"
    else:
        emoji, title = "🔴", f"Failure: {ti.task_id}"

    message = f"""
        <b>{emoji} {title}</b>
        <b>DAG:</b> {ti.dag_id}
        <b>Task:</b> {ti.task_id}
    """

    try:
        hook = TelegramHook(telegram_conn_id=TELEGRAM_CONN_ID)
        hook.send_message(
            {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
        )
    except Exception as e:
        print(f"Error sending via Telegram: {e}")


# Callbacks acionados pelo status da task
def notify_success(context):
    _send_telegram(context, "success")


def notify_failure(context):
    _send_telegram(context, "failure")


# Define os callbacks padrão para todas as tasks da DAG
DEFAULT_ARGS = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_failure,
    "on_success_callback": notify_success,
}
