import urllib
import celeryconfig
import os
from celery import Celery
from extend_the_kombu import *
access_key = os.environ.get("AWS_ACCESS_KEY_ID")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
broker_uri = 'sqs://%s:%s@' % (urllib.quote(access_key, safe=''),
                               urllib.quote(secret_key, safe=''))
app = Celery('app', broker=broker_uri)
app.config_from_object('celeryconfig')

@app.task(queue="bulk-message-extend")
def receive_message(msg, msg_1):
    print "Got the mesaage 0==>  ",  msg
    print "Got the mesaage 1==>  ",  msg_1
    print "message recivied successfully"



