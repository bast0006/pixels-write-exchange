import asyncio
from datetime import datetime
from typing import Optional


import aiohttp
from dotenv import dotenv_values
from pony import orm
from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

# NOTES ON WORKING WITH PONY AND ASYNCIO:
#  DO NOT AWAIT WITHIN orm.db_session() or YOU WILL CAUSE DEADLOCK OR CORRUPTION
#  Keep transaction windows short and sweet, like normal except more so.

RETURNED_TASK_COUNT = 10  # Number of tasks to return on GET /tasks
# these are dynamically updated on a timer
CANVAS_WIDTH = 208
CANVAS_HEIGHT = 117
API_BASE = "https://pixels.pythondiscord.com"
CONFIG = dotenv_values(".env")

API_KEY = CONFIG["API_KEY"]
ERROR_WEBHOOK = CONFIG["ERROR_WEBHOOK"]
MAGIC_AUTHORIZATION = CONFIG["MAGIC_AUTHORIZATION"]
HEADERS = {
    "Authorization": "Bearer " + API_KEY,
    "User-Agent": "bast-write-market/0.1",
}


async def homepage(request):
    return Response(
        "Hello world! And welcome to Bast's Pixel Write Exchange!\n"
        "All requests should have the 'Authorization' header set to a unique identifiable token of up to 30 characters that will be used for your balance."
        " Surrounding spaces will be stripped.\n"
        f"GET /tasks to get the top {RETURNED_TASK_COUNT} highest paying tasks. You may provide ?minimum_pay=<float> to filter.\n"
        '\tReturns: [{"id": task_id, "pay": task_pay},]\n'
        "GET /tasks/<taskid> to claim a task. This claim will last 30 seconds.\n"
        '\tReturns: {"id": task_id, "pay": task_pay, "x": x_coord, "y": y_coord, "color": hex_color}\n'
        "POST /tasks/<taskid> to submit a task. We will verify whether the pixel has changed, and reward you with your payment.\n"
        "\tWe check every 10 seconds (or roughly the maximum view ratelimit) for new pixels globally, and faster with /get_pixel on individual submissions if available. "
        "It may take up to that long for your submission to return, so plan accordingly.\n"
        "POST /tasks to create a task.\n"
        '\tFormat: {"pay": task_pay, "x": x_coord, "y": y_coord, "color": hex_color}\n'
        '\tReturns: {"id": new_task_id}\n'
    )


async def fetch_tasks(request):
    with orm.db_session():
        top_ten_payers = orm.select(task for task in Task if not task.completed).order_by(Task.pay)[:RETURNED_TASK_COUNT]
        top_ten_payers = [{"id": task.id, "pay": task.pay} for task in top_ten_payers]
    return JSONResponse(top_ten_payers)


db = orm.Database()


class User(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    identifier = orm.Required(str, index=True, unique=True)
    money = orm.Required(float, sql_default=0)
    total_tasks = orm.Required(int, sql_default=0)
    requested_tasks = orm.Set('Task', reverse='reservation')
    created_tasks = orm.Set('Task', reverse='creator')


class Task(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    creator = orm.Required(User)
    completed = orm.Required(bool, sql_default=False)
    x = orm.Required(int)
    y = orm.Required(int)
    color = orm.Required(str)
    pay = orm.Required(float)
    reservation = orm.Optional(User)
    reservation_expires = orm.Optional(datetime)
    reservation_task_id = orm.Optional(int)  # name of the asyncio task we use to cancel auto-expire


async def start_database():
    db.bind(provider='sqlite', filename='data.db', create_db=True)
    db.generate_mapping(create_tables=True)

    with orm.db_session():
        task_expiration_checks = orm.select(task for task in Task if task.reservation)
        for task in task_expiration_checks:
            assert task.reservation_expires is not None
            assert task.reservation_task_id is not None
            if task.reservation_expires < datetime.now():
                task.reservation = None
                task.reservation_expires = None
                task.reservation_task_id = None
            else:
                asyncio.create_task(expire_task(task.id, task.reservation_expires))


async def expire_task(task_id: int, time: datetime):
    time_to_sleep = (datetime.now() - datetime).total_seconds()
    await asyncio.sleep(time_to_sleep)
    with orm.db_session():
        task = Task[task_id]
        if not task.completed:
            task.reservation = None
            task.reservation_task_id = None
            task.reservation_expires = None
        else:
            return  # Successfully completed while we waited


async def canvas_size_loop():
    global CANVAS_WIDTH, CANVAS_HEIGHT
    TICK_RATE = 10  # every 10 seconds
    first = True
    while True:
        if not first:
            await asyncio.sleep(TICK_RATE)
            first = False
        async with aiohttp.ClientSession() as session:
            async with session.get(API_BASE + "/get_size", headers=HEADERS) as response:
                if result.status_code != 200:
                    await session.post(ERROR_WEBHOOK, json=make_embed("Error hit while getting canvas size:", status_code=result.status_code, error=await result.read()))
                    continue
                try:
                    result = await response.json()
                except Exception as e:
                    await session.post(ERROR_WEBHOOK, json=make_embed("Error while parsing /get_size json", error=str(e)))
                    continue

                CANVAS_WIDTH = result["width"]
                CANVAS_HEIGHT = result["height"]


async def start_size_loop():
    create_erroring_task(canvas_size_loop())


def create_erroring_task(coroutine):
    task = asyncio.create_task(coroutine)
    def ensure_exception(fut: asyncio.Future) -> None:
        """Ensure an exception in a task is raised without hard awaiting."""
        if fut.done() and not fut.cancelled():
            return
        fut.result()

    task.add_done_callback(ensure_exception)


def make_embed(content: str, **kwargs):
    """Quick and dirty make a discord embed dictionary"""
    embed = {}
    if content:
        embed["description"] = content
    if kwargs:
        embed["fields"] = []
    for key, value in kwargs.items():
        embed['fields'].append({"name": key, "value": str(value), "inline": False})
    return {"embeds": [embed]}

app = Starlette(
    debug=True,
    routes=[
        Route('/', homepage),
        Route('/tasks', fetch_tasks),
    ],
    on_startup=[start_database],
)
orm.set_sql_debug(True)
