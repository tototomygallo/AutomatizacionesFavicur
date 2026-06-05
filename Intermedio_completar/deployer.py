from pathlib import Path
from environs import Env
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import CronSchedule
from prefect.runner.storage import GitRepository
from flow import flujo_intermedio_940

# -----------------------------
# ENV
# -----------------------------

env = Env()
env.read_env()
ENV = env.str("ENV", "development")

print(ENV)

# -----------------------------
# CONFIG DEPLOY
# -----------------------------

DEPLOY_NAME = "Paso intermedio de metodos de pago"

ENTRYPOINT = "Intermedio_completar/flow.py:flujo_intermedio_940"

DEVELOPMENT_POOL_NAME = "local"
PRODUCTION_POOL_NAME = "favicur-pool"

# -----------------------------
# DEPLOY
# -----------------------------

if __name__ == "__main__":

    print("ENV:", ENV)
    print("PATH:", str(Path(__file__).parent))

    # -------------------------
    # DEVELOPMENT
    # -------------------------

    if ENV == "development":

        flujo_intermedio_940.from_source(
            source=str(Path(__file__).parent),
            entrypoint=ENTRYPOINT,
        ).deploy(
            name=DEPLOY_NAME,
            work_pool_name=DEVELOPMENT_POOL_NAME,
        )

    # -------------------------
    # PRODUCTION
    # -------------------------

    if ENV == "production":

        flujo_intermedio_940.from_source(
            source=GitRepository(
                url="https://github.com/tototomygallo/AutomatizacionesFavicur.git",
                credentials={
                    "access_token": Secret.load("githubtoken")
                },
                branch="main",
            ),
            entrypoint=ENTRYPOINT,
        ).deploy(
            name=DEPLOY_NAME,
            work_pool_name=PRODUCTION_POOL_NAME,
            schedules=[
                CronSchedule(
                    cron="40 9 * * *",
                    timezone="America/Argentina/Buenos_Aires"
                )
            ],
            paused=True,
            tags=["Excel", "Produccion"],
            ignore_warnings=True,
        )