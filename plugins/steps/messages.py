from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(telegram_conn_id='test',
                        token='6304595276:AAG3pGSKcG2c_BWMNG_KEFNsnV8QiUHXg0A',
                        chat_id='-4157862253')
    dag = context['task_instance_key_str']
    run_id = context['run_id']

    message = f'Исполненение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '-4157862253',
        'text': message
    }) # отправление сообщения 

def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                        token='6304595276:AAG3pGSKcG2c_BWMNG_KEFNsnV8QiUHXg0A',
                        chat_id='-4157862253')

    task_instance_key_str = context['task_instance_key_str']
    run_id = context['run_id']


    message = f'Исполненение DAG {task_instance_key_str} с id={run_id} не прошло!' # определение текста сообщения
    hook.send_message({
        'chat_id': '-4157862253',
        'text': message
    }) # отправление сообщения