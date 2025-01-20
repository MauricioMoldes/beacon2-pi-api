from pymongo.mongo_client import MongoClient
from beacon.connections.mongo import conf

if conf.database_cluster:
    uri = "mongodb+srv://{}:{}@{}/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000".format(
        conf.database_user,
        conf.database_password,
        conf.database_host
    )
else:
    uri = "mongodb://{}:{}@{}:{}/{}?authSource={}".format(
        conf.database_user,
        conf.database_password,
        conf.database_host,
        conf.database_port,
        conf.database_name,
        conf.database_auth_source
    )

if conf.database_certificate != '' and conf.database_cafile != '':
    uri += '&tls=true&tlsCertificateKeyFile={}&tlsCAFile={}'.format(conf.database_certificate, conf.database_cafile)

client = MongoClient(uri)