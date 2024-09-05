
from typing import Optional
from aiohttp import web
from aiohttp.web import FileField
from aiohttp.web_request import Request
from .plugins import DummyPermissions as PermissionsProxy
from beacon.logs.logs import LOG
from beacon.auth.__main__ import authentication
from beacon.logs.logs import log_with_args
from beacon.conf.conf import level
from beacon.utils.requests import check_request_content_type, get_qparams
from beacon.request.parameters import RequestParams
from beacon.connections.mongo.datasets import get_list_of_datasets
from bson import json_util
from beacon.response.granularity import build_beacon_error_response
import traceback

@log_with_args(level)
async def authorization(self, request):
    try:
        auth = request.headers.get('Authorization')
        if not auth or not auth.lower().startswith('bearer '):
            raise web.HTTPUnauthorized()
        list_visa_datasets=[]
        access_token = auth[7:].strip() # 7 = len('Bearer ')
        user, list_visa_datasets = await authentication(self, access_token)
        if user is None:
            user = 'public'
        elif user == 'public':
            username = 'public'
        else:
            username = user.get('preferred_username')
    except Exception:
        list_visa_datasets = []
        username = 'public'
        return username, list_visa_datasets
    return username, list_visa_datasets

@log_with_args(level)
async def get_datasets_list(self, request: Request, authorized_datasets):
    try:
        qparams = await get_qparams(self, request)
        specific_datasets_unauthorized = []
        search_and_authorized_datasets = []
        try:
            specific_datasets = qparams.query.request_parameters['datasets']
        except Exception:
            specific_datasets = []
        # Get response
        if specific_datasets != []:
            for element in authorized_datasets:
                if element in specific_datasets:
                    search_and_authorized_datasets.append(element)
            for elemento in specific_datasets:
                if elemento not in search_and_authorized_datasets:
                    specific_datasets_unauthorized.append(elemento)
            beacon_datasets = get_list_of_datasets(self)
            response_datasets = [ r['id'] for r in beacon_datasets if r['id'] in search_and_authorized_datasets]

        else:
            beacon_datasets = get_list_of_datasets(self)
            LOG.debug(beacon_datasets)
            LOG.debug(type(beacon_datasets))
            specific_datasets = [ r['id'] for r in beacon_datasets if r['id'] not in authorized_datasets]
            response_datasets = [ r['id'] for r in beacon_datasets if r['id'] in authorized_datasets]
            specific_datasets_unauthorized.append(specific_datasets)
    except Exception:
        raise
    return response_datasets, qparams

def dataset_permissions(func):
    @log_with_args(level)
    async def permission(self, request: Request):
        try:
            post_data = await check_request_content_type(self, request)

            v = post_data.get('datasets')
            if v is None:
                requested_datasets = []
            elif isinstance(v, list):
                requested_datasets = v
            elif isinstance(v, FileField):
                requested_datasets = []
            else:
                requested_datasets = v.split(sep=',')
            
            username, list_visa_datasets = await authorization(self, request)
                
            datasets = await PermissionsProxy.get(self, username=username, requested_datasets=requested_datasets)
            dict_returned={}
            dict_returned['username']=username
            authorized_datasets=list(datasets)
            for visa_dataset in list_visa_datasets:
                authorized_datasets.append(visa_dataset)
            response_datasets, qparams = await get_datasets_list(self, request, authorized_datasets)
            return await func(self, request, response_datasets, qparams)
        except Exception:
            raise
    return permission

