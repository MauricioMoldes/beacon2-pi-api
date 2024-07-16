'''

from typing import Optional
from aiohttp import web
from aiohttp.web import FileField
from aiohttp.web_request import Request
from .plugins import DummyPermissions as PermissionsProxy
from beacon.logs.logs import log_with_args
from beacon.conf.conf import level
from beacon.auth.__main__ import authentication
from beacon.logs.logs import LOG

@log_with_args(level=level)
async def authorization(request):
    try:
        auth = request.headers.get('Authorization')
        if not auth or not auth.lower().startswith('bearer '):
            raise web.HTTPUnauthorized()
        list_visa_datasets=[]
        access_token = auth[7:].strip() # 7 = len('Bearer ')
        user, list_visa_datasets = await authentication(access_token)
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

@log_with_args(level=level)
def dataset_permissions(func):
    @log_with_args(level=level)
    async def permission(request: Request):
        if request.headers.get('Content-Type') == 'application/json':
            post_data = await request.json()
        else:
            post_data = await request.post()

        v = post_data.get('datasets')
        if v is None:
            requested_datasets = []
        elif isinstance(v, list):
            requested_datasets = v
        elif isinstance(v, FileField):
            requested_datasets = []
        else:
            requested_datasets = v.split(sep=',')
        
        username, list_visa_datasets = await authorization(request)
        LOG.debug(username)
            
        datasets = await PermissionsProxy.get(self=PermissionsProxy, username=username, requested_datasets=requested_datasets)
        dict_returned={}
        dict_returned['username']=username
        datasets=list(datasets)
        for visa_dataset in list_visa_datasets:
            datasets.append(visa_dataset)
        dict_returned['datasets']=list(datasets)

        return await func(request, dict_returned)
    return permission

'''