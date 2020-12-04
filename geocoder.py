import requests
import re
import psycopg2

from trans_util import bd09_to_wgs84
from ..database.news_server_mongo import MongoDBPipeline
from setting import POSTGRESQL_CON

mongo = MongoDBPipeline()
region = mongo.db['region']

regionObjectList = list(region.find({}))
provinceObjectList = [o for o in regionObjectList if o['level'] == 'province']
cityObjectList = [o for o in regionObjectList if o['level'] == 'city']
districtObjectList = [o for o in regionObjectList if o['level'] == 'district']


# 区域名称简写匹配规则
subPattern = r'((省)|(市)|(县)|(区)|(自治州)|(自治县)|(自治区)|(自治旗)|(林区))$'
raceList = ['满族', '回族', '达斡尔族', '蒙古族', '朝鲜族', '土家族', '苗族', '瑶族', ' 壮族', '黎族']


def region_object_in_content(content, regionObject):
    if regionObject['name'] == '':
        return False

    if regionObject['name'] in content:
        return True
    if len(regionObject['name']) >= 3:
        nameTrimmed = re.sub(
            subPattern, '', regionObject['name'])
        if nameTrimmed == '':
            return False
        if nameTrimmed in content:
            return True

        for race in raceList:
            nameTrimmed = nameTrimmed.replace(race, '')

        if nameTrimmed == '':
            return False
        if nameTrimmed in content:
            return True

    return False


def region_name_like_name(name, region):
    if name == '' or region == '':
        return False
    if name == region:
        return True
    if len(region) >= 3 and len(name) >= 2:
        nameTrimmed = re.sub(
            subPattern, '', region)
        if nameTrimmed == name:
            return True

    return False


def geocode_region(item):
    '''
    解析省、市、区
    '''

    # 新闻内容
    content = item['content']

    item['province_list'] = []
    item['city_list'] = []
    item['district_list'] = []

    # 查找区域名称，添加入对应level的列表
    for regionObject in regionObjectList:
        if region_object_in_content(content, regionObject):
            item[regionObject['level'] + '_list'].append(regionObject['name'])

    # 城市对应的省份添加入省份列表
    for cityName in item['city_list']:
        cityObject = [o for o in cityObjectList if o['name'] == cityName][0]
        provinceCode = cityObject['parent']['adcode']
        provinceObject = [
            o for o in provinceObjectList if o['adcode'] == provinceCode][0]
        item['province_list'].append(provinceObject['name'])

    # 判断区列表中的区是否合理，遍历区列表

    # TODO: 区对应省份的合理性规则添加

    for districtName in item['district_list']:
        # 多个区可能同名，因此要当作列表进行处理。列表中包含的是名称相同的区
        districtSingleObjectList = [
            o for o in districtObjectList if o['name'] == districtName]

        # 区存在合理标志
        isReasonable = False
        for districtObject in districtSingleObjectList:

            cityCode = districtObject['parent']['adcode']
            cityObject = [
                o for o in cityObjectList if o['adcode'] == cityCode][0]

            # 判断城市列表中，是否存在区对应的城市
            if cityObject['name'] in item['city_list']:
                # 存在则打上标签
                isReasonable = True
                break
            else:
                # 不存在则，记录区对应的城市
                if item.get('debug', None) is None:
                    item['debug'] = [cityObject['name']]
                else:
                    item['debug'].append(cityObject['name'])

        # 如果合理，区不用变动。如果不合理，则把区列表中的区删除
        if not isReasonable:
            item['district_list'] = [o for o in item['district_list']
                                     if o != districtName]

    item['province_list'] = list(set(item['province_list']))
    item['city_list'] = list(set(item['city_list']))
    item['district_list'] = list(set(item['district_list']))

    return item


def geocode_ner(item):
    '''
    根据NER的结果进行地理编码
    1. 根据region的解析结果，去掉冗余数据
    2. 按照之前的方法解析
    '''
    # 1. 非空校验
    location_ls = list(set(item['location_ner'] + item['institute_ner']))
    if len(location_ls):
        # 记录地理编码结果
        item['provinces'] = dict()
        item['cities'] = dict()
        item['locations_bd09'] = dict()
        item['locations'] = dict()

        # 记录地理编码信息
        item['geocode_msg'] = dict()
    else:
        return item

    # 2. 去冗余
    region_ls = item['province_list'] + \
        item['city_list'] + item['district_list']
    # 遍历时删除，用副本遍历
    for location_name in location_ls[:]:
        for region_name in region_ls:
            if region_name_like_name(location_name, region_name):
                # 芜湖存在芜湖市和芜湖县。删除前再次判断
                if location_name not in location_ls:
                    continue
                location_ls.remove(location_name)

    # 3. 地址解析
    if len(item['city_list']) >= 1:
        geocode_city_list = item['city_list']
    elif len(item['provinces']) >= 1:
        geocode_city_list = item['province_list']
    else:
        geocode_city_list = []

    # TODO: 全国性、全省性地名的解析
    if len(geocode_city_list) == 0:
        return item

    # 外层循环，遍历地名
    for location_name in location_ls:

        # 记录地名的编码结果，初始结果评价指标
        geocode_msg = {}
        item['geocode_msg'][location_name] = geocode_msg
        result_metrics = {'comprehension': 0, 'confidence': 0}

        # 第二层循环，遍历地名可能对应的城市
        for geocode_city in geocode_city_list:

            r = requests.get(
                f'http://api.map.baidu.com/geocoding/v3/?city={geocode_city}'
                f'&address={location_name}&output=json'
                f'&ak=xSCBGWXWcIQ5VRg1omPYWpcgtAySsMYE'
            )
            json = r.json()

            geocode_msg[geocode_city] = json
            if (json['status'] is None) or (json['status'] != 0):
                continue

            result = json['result']

            if result['comprehension'] >= 70 and result['confidence'] >= 20:
                longitude, latitude = (
                    result['location']['lng'], result['location']['lat'])

                # 判断是否已经存在该地点了
                if location_name in item['locations_bd09']:
                    # 存在，则比较评价指标，决定是否替换
                    if (result['comprehension'] >= result_metrics['comprehension']
                        and
                            result['confidence'] >= result_metrics['confidence']):

                        item['locations_bd09'][location_name] = {
                            'longitude': longitude, 'latitude': latitude}
                        trans = bd09_to_wgs84(longitude, latitude)
                        item['locations'][location_name] = {
                            'longitude': trans[0], 'latitude': trans[1]}

                else:
                    # 不存在，设置评价指标，并赋值
                    result_metrics['comprehension'] = result['comprehension']
                    result_metrics['confidence'] = result['confidence']

                    item['locations_bd09'][location_name] = {
                        'longitude': longitude, 'latitude': latitude}
                    trans = bd09_to_wgs84(longitude, latitude)
                    item['locations'][location_name] = {
                        'longitude': trans[0], 'latitude': trans[1]}


def scale(item, postgis=True):
    '''
    计算尺度、跨度

    定义尺度：
    0：不存在地点名词
    1：区级
    2：多区级、市级
    3：多市级、省级
    4：多省级、全国级

    跨度：
    locations中所有点形成的外接矩形？应该是圆形
    利用PostGIS计算
    '''

    # 根据省、市判断尺度
    if len(item['province_list']) > 1:
        item['scale'] = 4
    elif len(item['city_list']) > 1:
        item['scale'] = 3
    elif len(item['district_list']) > 1:
        item['scale'] = 2
    elif len(item['district_list']) == 1:
        item['scale'] = 1
    else:
        item['scale'] = 0

    # 如果不使用PostGIS计算跨度，直接返回
    if not postgis:
        return item

    # 不存在地址 或 只有一个
    if len(item['locations']) <= 1:
        item['span'] = 0
    else:
        conn = psycopg2.connect(**POSTGRESQL_CON)
        cur = conn.cursor()

        query = (
            f'SELECT (ST_MinimumBoundingRadius(ST_Collect(f.geom))).radius '
            f'from (select geom from location where news_id = \'{item["_id"]}\') as f;'
        )

        cur.execute(query)
        item['span'] = cur.fetchone()[0]

        cur.close()
        conn.close()


def geocode_origin(item, city='武汉'):
    '''
    原地理编码方法
    从location_ner中利用高德API解析省、市
    再根据市利用百度API解析相应的地址
    '''

    # 1. 非空校验
    location_ls = list(set(item['location_ner'] + item['institute_ner']))
    if len(location_ls):
            # 记录地理编码结果
        item['provinces'] = dict()
        item['cities'] = dict()
        item['locations_bd09'] = dict()
        item['locations'] = dict()

        # 记录地理编码信息
        item['geocode_msg'] = dict()
    else:
        return item

    # 2. 省、市解析
    for location_name in item['location_ner']:

        # “|”在参数中为地址分隔符。
        if '|' in location_name:
            continue

        r = requests.get(
            f'https://restapi.amap.com/v3/geocode/geo?'
            f'key=6172ea799c64fdc98eed0bdd4869f3fc&'
            f'address={location_name}'
        )
        json = r.json()
        item['geocode_msg'][location_name] = json

        if ('status' not in json) or (json['status'] != '1') or (json['count'] != '1'):
            continue

        level = json['geocodes'][0]['level']
        formatted_address = json['geocodes'][0]['formatted_address']
        longitude, latitude = json['geocodes'][0]['location'].split(',')
        if level == '省':
            item['provinces'][formatted_address] = {
                'longitude': longitude,
                'latitude': latitude,
            }
            location_ls.remove(location_name)
        if level == '市':
            item['cities'][formatted_address] = {
                'longitude': longitude,
                'latitude': latitude,
            }
            location_ls.remove(location_name)

    # 3. 地址解析
    if len(item['cities']) == 1:
        # 重大错误！！！前面爬的结果全错了...
        # city = list(item['cities'].items())[0]
        city = list(item['cities'].keys())[0]
    elif len(item['provinces']) == 1:
        city = list(item['provinces'].keys())[0]
    for location_name in location_ls:
        r = requests.get(
            f'http://api.map.baidu.com/geocoding/v3/?city={city}'
            f'&address={location_name}&output=json'
            f'&ak=xSCBGWXWcIQ5VRg1omPYWpcgtAySsMYE'
        )
        json = r.json()

        item['geocode_msg'][location_name] = json
        if (json['status'] is None) or (json['status'] != 0):
            continue

        result = json['result']

        if result['comprehension'] >= 70 and result['confidence'] >= 20:
            longitude, latitude = (
                result['location']['lng'], result['location']['lat'])
            item['locations_bd09'][location_name] = {
                'longitude': longitude, 'latitude': latitude}

            trans = bd09_to_wgs84(longitude, latitude)
            item['locations'][location_name] = {
                'longitude': trans[0], 'latitude': trans[1]}


def scale_origin(item, postgis=True):
    '''
    计算尺度、跨度

    临时定义尺度：
    0：不存在地点名词
    1：市级以下
    2：单市级
    3：多市级、省级
    4：多省级

    跨度：
    没有地址则为0
    locations中所有点形成的外接矩形？应该是圆形
    利用PostGIS计算
    '''

    # 如果不存在locations字段，表明没有识别出任何地点名词
    if 'locations' not in item:
        item['scale'] = 0
        item['span'] = 0
        return item

    # 根据省、市判断尺度
    if len(item['provinces']) > 1:
        item['scale'] = 4
    elif len(item['provinces']) == 1:
        item['scale'] = 3
    elif len(item['cities']) > 1:
        item['scale'] = 3
    elif len(item['cities']) == 1:
        item['scale'] = 2
    else:
        item['scale'] = 1

    # 如果不使用PostGIS计算跨度，直接返回
    if not postgis:
        return item

    # 不存在地址 或 只有一个
    if len(item['locations']) <= 1:
        item['span'] = 0
    else:
        conn = psycopg2.connect(
            host='data.piaoyang.tk', dbname='news', user='piaoyang', password='123456')
        cur = conn.cursor()

        query = (
            f'SELECT (ST_MinimumBoundingRadius(ST_Collect(f.geom))).radius '
            f'from (select geom from location where news_id = \'{item["_id"]}\') as f;'
        )

        cur.execute(query)
        item['span'] = cur.fetchone()[0]

        cur.close()
        conn.close()
