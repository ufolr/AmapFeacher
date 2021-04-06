import configparser
import asyncio
from asyncio import Queue
import aiohttp
import json
import copy
from aiolimiter import AsyncLimiter
import datetime
import os


class TaskAmapPoi:
    bese_url = 'http://restapi.amap.com/v3/place/polygon?key=%s&polygon=%s&types=%s&extensions=all&offset=%d&page=%d'

    def __init__(self, key, rect, poi_type, page_num=1, page_size=20, check=False):
        self.key = key
        self.rect = rect
        self.poi_type = poi_type
        self.page_num = page_num
        self.page_size = page_size
        self.check = check

    def to_url(self):
        return TaskAmapPoi.bese_url % (self.key, self.rect.to_amap_str(), self.poi_type, self.page_size, self.page_num)

    def copy(self):
        return copy.deepcopy(self)


class Rec:
    """
    用于划分高德地图的矩形格子
    高德经纬度小数精度为6位,剖分时小数超过5位(米级)则相应的扩大网格,保证网格为米级精度
    """

    def __init__(self, xmin, ymin, xmax, ymax):
        self.xmin = round(xmin - 0.000005, 5) - 0.00001
        self.ymin = round(ymin - 0.000005, 5) - 0.00001
        self.xmax = round(xmax + 0.000005, 5) + 0.00001
        self.ymax = round(ymax + 0.000005, 5) + 0.00001

    def quad(self):
        xmid = (self.xmax + self.xmin)/2
        ymid = (self.ymax + self.ymin)/2
        # 按象限顺序存储
        return [Rec(xmid, ymid, self.xmax, self.ymax),
                Rec(xmid, self.ymin, self.xmax, ymid),
                Rec(self.xmin, self.ymin, xmid, ymid),
                Rec(self.xmin, ymid, xmid, self.ymax)]

    def to_amap_str(self):
        return "%f,%f,%f,%f" % (self.xmin, self.ymin, self.xmax, self.ymax)


async_clint = None
max_page_size = 20
async_limit = None


async def async_get(url, retry=5):
    """
    请求指定域名,获取返回结果的json
    """
    try:
        async with async_limit:
            async with async_clint.get(url, timeout=300) as response:
                if response.status in (200, 429,):
                    json = await response.json()
                    if json.get("infocode") == '10000':
                        return json
                    else:
                        err_msg = "AMAP_ERROR: %s-%s" % (
                            json.get("infocode"), json.get("info"))
                        raise aiohttp.ClientResponseError(
                            response.request_info, response.history,
                            message=err_msg)
                else:
                    print("HTTP_ERROR: %s" % response.status)
                    raise aiohttp.ClientResponseError()
    except Exception as err:
        if retry > 0:
            await asyncio.sleep(pow(2, 6-retry))
            return await async_get(url, retry-1)
        else:
            print("Retry out of chances: %s" % err)
            return None


async def request_poi_in_rec(task):
    url = task.to_url()
    return await async_get(url)


async def query_poi(task_queue, output):
    task = await task_queue.get()
    resp_json = await request_poi_in_rec(task)
    if resp_json:
        # 如果总数大于720进行剖分
        if int(resp_json.get('count')) > 720:
            for new_rect in task.rect.quad():
                new_task = task.copy()
                new_task.rect = new_rect
                new_task.check = True
                await task_queue.put(new_task)
            print("meshing rect: %s" % task.rect.to_amap_str())
        elif task.check == True:
            task.check = False
            task.page_size = max_page_size
            await task_queue.put(task)
        else:
            # 如果不用剖分则记录结果
            pois = resp_json.get('pois')
            for poi in pois:
                id = poi.get('id')
                output[id] = poi
            if len(pois) == task.page_size:
                task.page_num += 1
                await task_queue.put(task)
            if task.page_num > (720 / max_page_size):
                print('some thing wrong')
            print("query rect: %s, page:%d" %
                  (task.rect.to_amap_str(), task.page_num))


async def query_pois(task_queue, output):
    while True:
        try:
            await asyncio.wait_for(query_poi(task_queue, output), timeout=20)
        except asyncio.TimeoutError as err:
            print('timeout: coroutine exit.')
            return
        await asyncio.sleep(0.5)
    return


async def init_task_queue(key, rect, typelist, task_queue, output):
    """
    初始化请求任务队列
    """
    for typ in typelist:
        await task_queue.put(TaskAmapPoi(key, rect, typ, page_size=2, check=True))


async def save_data(pois, file_name):
    with open(file_name, 'w', encoding='utf-8') as f:
        for poi in pois:
            poi_data = await clean_metro_data(poi)
            if poi_data:
                line = json.dumps(poi_data, ensure_ascii=False)
                f.write(line+'\n')
            else:
                print('Failed clean data')
        f.flush()


async def clean_metro_data(json):
    poi_type = json.get('typecode')
    if poi_type[:4] == '1505':
        if '|' in poi_type:
            print('oh no')
    if poi_type == '150500' or poi_type == '150501':
        metro_poi_type = '0' if poi_type == '150501' else '1'
        geopoint = [float(x) for x in json.get('location').split(',')]
        data = {'id': json.get('id'),
                'parent': json.get('parent'),
                'name': json.get('name'),
                'line': json.get('address'),
                'geopoint': geopoint,
                'province': json.get('pname'),
                'city': json.get('cityname'),
                'county': json.get('adname'),
                'type': metro_poi_type}
        return data
    print(poi_type)
    return None


async def main(loop, num_consumers):
    global async_clint, async_limit

    conn = aiohttp.TCPConnector(
        verify_ssl=False, limit=100, use_dns_cache=True)
    async_clint = aiohttp.ClientSession(
        loop=loop, connector=conn, conn_timeout=30, read_timeout=30)
    async_limit = AsyncLimiter(20, 0.1)

    # congif
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')
    data_dir = config.get('amap_spider', 'data_path')
    key = config.get('amap_spider', 'key')
    poi_types = json.loads(config.get('amap_spider', 'poi_types'))
    target_rect = json.loads(config.get('amap_spider', 'target_rect'))

    # do query
    async with async_clint:
        rect = Rec(*target_rect)
        task_queue = Queue()
        output = dict()
        init = loop.create_task(init_task_queue(
            key, rect, poi_types, task_queue, output))

        tasks = [
            loop.create_task(query_pois(task_queue, output))
            for i in range(100)
        ]
        await asyncio.wait(tasks + [init])

    # save data
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    file_date = datetime.datetime.now().strftime("%Y%m%d")
    await save_data(output.values(), '%s/metro_%s.json' % (data_dir, file_date))
    print(len(output))


event_loop = asyncio.get_event_loop()
try:
    event_loop.run_until_complete(main(event_loop, 2))
finally:
    event_loop.close()
