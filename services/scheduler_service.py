import atexit
import os

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


def configure_and_start_scheduler(app, executar_tarefas_diarias, executar_lembretes_reunioes, executar_lembretes_aniversarios):
    scheduler = BackgroundScheduler()

    scheduler.add_job(
        func=executar_tarefas_diarias,
        trigger=CronTrigger(hour=8, minute=0),
        id="tarefas_diarias",
        replace_existing=True,
    )
    scheduler.add_job(
        func=executar_lembretes_reunioes,
        trigger=CronTrigger(hour=18, minute=0),
        id="lembretes_reunioes_tarde",
        replace_existing=True,
    )
    scheduler.add_job(
        func=executar_lembretes_aniversarios,
        trigger=CronTrigger(hour=6, minute=0),
        id="lembretes_aniversarios",
        replace_existing=True,
    )

    if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        scheduler.start()
        print("✅ Scheduler iniciado com sucesso!")
        print("   - Tarefas diárias: 08:00")
        print("   - Lembretes de reuniões: 18:00")
        print("   - Aniversários: 06:00")
        atexit.register(lambda: scheduler.shutdown())
    else:
        print("ℹ️ Scheduler não iniciado no processo pai do reloader (debug).")

    return scheduler
