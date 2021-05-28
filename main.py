from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response
from starlette.routing import Route


async def homepage(request):
    return Response(
        "Hello world! And welcome to Bast's Pixel Write Exchange!\n"
        "All requests should have the 'Authorization' header set to a unique identifiable token of up to 30 characters that will be used for your balance. Surrounding spaces will be stripped.\n"
        "GET /tasks to get the top ten highest paying tasks. You may provide ?minimum_pay=<float> to filter.\n"
        '\tFormat: {"id": task_id, "pay": task_pay, "x": x_coord, "y": y_coord, "color": hex_color}'
        "GET /tasks/<taskid> to claim a task. This claim will last 30 seconds.\n"
        "POST /tasks/<taskid> to submit a task. We will verify whether the pixel has changed, and reward you with your payment.\n"
        "POST /tasks to create a task. "
    )


app = Starlette(debug=True, routes=[
    Route('/', homepage),
])
