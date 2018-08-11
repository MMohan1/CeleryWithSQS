import socket
from kombu.transport import SQS, TRANSPORT_ALIASES, virtual
from kombu.asynchronous.aws.sqs.message import AsyncMessage
from kombu.asynchronous.aws.ext import  exceptions
from kombu.utils.encoding import bytes_to_str
from kombu.utils.json import loads, dumps
from message_flow import SQSLibExtended
import uuid
import json
import os
import base64

DEFULT_AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
DEFULT_BUCKET_NAME = os.environ.get("AWS_BUCKET_NAME")

class SQSClientExtended(SQS.Channel):

    
    def _put(self, queue, message, **kwargs):
        """Put message onto queue."""
        q_url = self._new_queue(queue)
        access_key = self.conninfo.userid
        secret_key = self.conninfo.password
        sle = SQSLibExtended(access_key, secret_key, DEFULT_AWS_REGION, DEFULT_BUCKET_NAME)
        body_data = json.loads(base64.b64decode(message["body"]))
        body_data[0] = json.loads(sle.get_message(json.dumps(body_data[0])))
        message["body"] =  body_data
        message["body"] = base64.b64encode(json.dumps(message["body"]))
        kwargs = {'QueueUrl': q_url,
                  'MessageBody': AsyncMessage().encode(dumps(message))}
        if queue.endswith('.fifo'):
            if 'MessageGroupId' in message['properties']:
                kwargs['MessageGroupId'] = \
                    message['properties']['MessageGroupId']
            else:
                kwargs['MessageGroupId'] = 'default'
            if 'MessageDeduplicationId' in message['properties']:
                kwargs['MessageDeduplicationId'] = \
                    message['properties']['MessageDeduplicationId']
            else:
                kwargs['MessageDeduplicationId'] = str(uuid.uuid4())
        self.sqs.send_message(**kwargs)


    def _message_to_python(self, message, queue_name, queue):
        body = base64.b64decode(message['Body'].encode())
        payload = loads(bytes_to_str(body))
        data = json.loads(base64.b64decode(payload["body"]))
        args = data[0]
        # from celery.contrib import rdb;rdb.set_trace()
        if isinstance(args, dict) and args.get('s3_refrance'):
            access_key = self.conninfo.userid
            secret_key = self.conninfo.password
            sle = SQSLibExtended(access_key, secret_key, DEFULT_AWS_REGION, DEFULT_BUCKET_NAME)
            actual_args = sle.receive_message(args)
            actual_args_list = json.loads(actual_args)
            data[0] = actual_args_list
            encode_data = base64.b64encode(json.dumps(data))
            payload["body"] = encode_data
        if queue_name in self._noack_queues:
            queue = self._new_queue(queue_name)
            self.asynsqs.delete_message(queue, message['ReceiptHandle'])
        else:
            try:
                properties = payload['properties']
                delivery_info = payload['properties']['delivery_info']
            except KeyError:
                # json message not sent by kombu?
                delivery_info = {}
                properties = {'delivery_info': delivery_info}
                payload.update({
                    'body': bytes_to_str(body),
                    'properties': properties,
                })
            # set delivery tag to SQS receipt handle
            delivery_info.update({
                'sqs_message': message, 'sqs_queue': queue,
            })
            properties['delivery_tag'] = message['ReceiptHandle']
        return payload

class ExtendedTransport(virtual.Transport):
    """SQS Transport."""

    Channel = SQSClientExtended

    polling_interval = 1
    wait_time_seconds = 0
    default_port = None
    connection_errors = (
        virtual.Transport.connection_errors +
        (exceptions.BotoCoreError, socket.error)
    )
    channel_errors = (
        virtual.Transport.channel_errors + (exceptions.BotoCoreError,)
    )
    driver_type = 'sqs'
    driver_name = 'sqs'

    implements = virtual.Transport.implements.extend(
        asynchronous=True,
        exchange_type=frozenset(['direct']),
    )

    @property
    def default_connection_params(self):
        return {'port': self.default_port}
TRANSPORT_ALIASES["SQS"] = "extend_the_kombu:ExtendedTransport"
TRANSPORT_ALIASES["sqs"] = "extend_the_kombu:ExtendedTransport"