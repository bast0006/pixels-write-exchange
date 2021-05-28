import asyncio
import random
from datetime import datetime, timedelta
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
EXPIRATION_OFFSET = timedelta(minutes=30)
# these are dynamically updated on a timer
CANVAS_WIDTH = 190  # 208
CANVAS_HEIGHT = 117
API_BASE = "https://pixels.pythondiscord.com"
CONFIG = dotenv_values(".env")

API_KEY = CONFIG["API_KEY"]
INFO_WEBHOOK = CONFIG["INFO_WEBHOOK"]
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
        '\tReturns: {"id": task_id, "pay": task_pay, "x": x_coord, "y": y_coord, "color": hex_color, "expires": expiration_time}\n'
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


async def create_task(request):
    authorization = request.headers.get('Authorization', None)
    if not authorization or not authorization.strip():
        return Response("Authorization is required for this endpoint.", status_code=401)
    elif len(authorization.strip()) > 30:
        return Response("Auth tokens must be 30 characters or less in size", status_code=401)

    data = await request.json()

    invalid_keys = set(data) - {'id', 'pay', 'x', 'y', 'color'}
    if invalid_keys:
        return Response("Invalid keys in data: " + " ,".join(invalid_keys), status_code=400)

    try:
        x = int(data['x'])
        if x < 0:
            return Response(f"Invalid x value '{x}': must be greater than or equal to zero", status_code=400)
        if x >= CANVAS_WIDTH:
            return Response(f"Invalid x value '{x}': must be less than {CANVAS_WIDTH}", status_code=400)
    except ValueError:
        return Response(f"Invalid x value '{data['x']}': must be convertible to an integer", status_code=400)

    try:
        y = int(data['y'])
        if y < 0:
            return Response(f"Invalid y value '{y}': must be greater than or equal to zero", status_code=400)
        if y >= CANVAS_HEIGHT:
            return Response(f"Invalid y value '{y}': must be less than {CANVAS_HEIGHT}", status_code=400)
    except ValueError:
        return Response(f"Invalid y value '{data['y']}': must be convertible to an integer", status_code=400)

    color = data['color'].strip().lower()
    if len(color) != 6:
        return Response(f"Invalid color '{color}': colors must be 6 characters long", status_code=400)

    bad_chars = set(color) - set("0123456789abcdef")
    if bad_chars:
        return Response(f"Invalid color: '{color}' must not have the characters '{repr(''.join(bad_chars))}")

    try:
        pay = float(data['pay'])
    except ValueError:
        return Response("Invalid payment offer: must be convertible to a number", status_code=400)

    with orm.db_session():
        user = User.get_from_authorization(authorization)
        if user.money < pay:
            return Response("Invalid payment offer: pay must be less than what you current have banked.", status_code=400)

        new_task = Task(
            creator=user,
            x=x,
            y=y,
            color=color,
            pay=pay,
        )

        orm.commit()
        response_json = {"id": new_task.id}
        if random.random() < 0.5:
            response_json["message"] = "Thanks for making the world a better place!"

    async with aiohttp.ClientSession() as session:
        await session.post(INFO_WEBHOOK, json=make_embed("New task created!", id=new_task.id, x=x, y=y, pay=pay, color=color, user=user.id))

    return JSONResponse(response_json)


async def reserve_task(request):
    authorization = request.headers.get('Authorization', None)
    if not authorization or not authorization.strip():
        return Response("Authorization is required for this endpoint.", status_code=401)
    elif len(authorization.strip()) > 30:
        return Response("Auth tokens must be 30 characters or less in size", status_code=401)

    task_id = request.path_params['task']

    with orm.db_session():
        user = User.get_from_authorization(authorization)
        task = Task.get(id=task_id)
        if not task:
            return Response(f"Invalid reserve request: task id '{task_id}' does not exist.", status_code=400)

        if task.completed:
            return Response(f"That task (id '{task.id}') has already been completed.", status_code=400)

        if task.reservation:
            return Response(f"That task (id '{task.id}') has already been reserved.", status_code=410)

        task.reservation = user
        task.reservation_expires = datetime.utcnow() + EXPIRATION_OFFSET
        expiration_task = asyncio.create_task(expire_task(reserve_task.NEXT_TASK_ID, task.reservation_expires))
        task.reservation_task_id = reserve_task.NEXT_TASK_ID
        reserve_task.EXPIRATION_TASKS[task.reservation_task_id] = expiration_task
        reserve_task.NEXT_TASK_ID += 1

    async with aiohttp.ClientSession() as session:
        await session.post(INFO_WEBHOOK, json=make_embed("Task reserved!", id=task.id, x=task.x, y=task.y, pay=task.pay, color=task.color, by=user.id))

    return JSONResponse({"id": task.id, "x": task.x, "y": task.y, "color": task.color, "pay": task.pay, "expires": task.reservation_expires.isoformat()+"Z"})

reserve_task.NEXT_TASK_ID = 1
reserve_task.EXPIRATION_TASKS = {}






async def submit_task(request):
    pass


db = orm.Database()


class User(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    identifier = orm.Required(str, index=True, unique=True)
    money = orm.Required(float, default=0)
    total_tasks = orm.Required(int, default=0)
    requested_tasks = orm.Set('Task', reverse='reservation')
    created_tasks = orm.Set('Task', reverse='creator')

    @classmethod
    def get_from_authorization(cls, authorization: str) -> 'User':
        authorization = authorization.strip()
        user = cls.get(identifier=authorization)
        if not user and authorization == MAGIC_AUTHORIZATION:
            # initial user seed
            return cls(identifier=authorization, money=30)
        return user or cls(identifier=authorization)


class Task(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    creator = orm.Required(User)
    completed = orm.Required(bool, default=False)
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
                create_erroring_task(expire_task(task.id, task.reservation_expires))


async def expire_task(task_id: int, when: datetime):
    time_to_sleep = (datetime.utcnow() - when).total_seconds()
    await asyncio.sleep(time_to_sleep)
    with orm.db_session():
        task = Task[task_id]
        if not task.completed:
            reserver = task.reservation
            task.reservation = None
            task.reservation_expires = None
            if task.reservation_task_id in reserve_task.EXPIRATION_TASKS:
                task_task = reserve_task.EXPIRATION_TASKS[task.reservation_task_id]
                del reserve_task.EXPIRATION_TASKS[task.reservation_task_id]
                task_task.cancel()
            task.reservation_task_id = None
        else:
            return  # Successfully completed while we waited

    async with aiohttp.ClientSession() as session:
        await session.post(INFO_WEBHOOK, json=make_embed("Task reservation expired", id=task.id, x=task.x, y=task.y, pay=task.pay, color=task.color, by=reserver))



async def canvas_size_loop():
    global CANVAS_WIDTH, CANVAS_HEIGHT
    TICK_RATE = 10  # every 10 seconds
    first = True
    while True:
        print("Size loop tick")
        if first:
            first = False
        else:
            await asyncio.sleep(TICK_RATE)
        await asyncio.sleep(1)
        async with aiohttp.ClientSession() as session:
            async with session.get(API_BASE + "/get_size", headers=HEADERS) as response:
                if response.status != 200:
                    await session.post(INFO_WEBHOOK, json=make_embed("Error hit while getting canvas size:", status_code=response.status, error=await result.read()))
                    continue
                try:
                    result = await response.json()
                except Exception as e:
                    await session.post(INFO_WEBHOOK, json=make_embed("Error while parsing /get_size json", error=str(e)))
                    continue

                if (CANVAS_WIDTH, CANVAS_HEIGHT) != (result["width"], result["height"]):
                    CANVAS_WIDTH = result["width"]
                    CANVAS_HEIGHT = result["height"]

                    await session.post(INFO_WEBHOOK, json=make_embed("Setting canvas size:", width=CANVAS_WIDTH, height=CANVAS_HEIGHT))


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
        embed['fields'].append({"name": key, "value": str(value), "inline": True})
    embed['timestamp'] = datetime.utcnow().isoformat()+"Z"
    return {"embeds": [embed]}


app = Starlette(
    debug=True,
    routes=[
        Route('/', homepage),
        Route('/tasks', fetch_tasks, methods=['GET']),
        Route('/tasks', create_task, methods=['POST']),
        Route('/tasks/{task:int}', reserve_task, methods=['GET']),
    ],
    on_startup=[start_database, start_size_loop],
)
orm.set_sql_debug(True)
