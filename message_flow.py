import uuid
import json
import base64
from enum import Enum
import boto3
from datetime import datetime, timedelta
from boto3.session import Session
from kombu.transport import SQS

class SQSExtendedClientConstants(Enum):
    DEFAULT_MESSAGE_SIZE_THRESHOLD = 262144
    MAX_ALLOWED_ATTRIBUTES = 10 - 1  # 10 for SQS, 1 for the reserved attribute
    RESERVED_ATTRIBUTE_NAME = "SQSLargePayloadSize"
    S3_BUCKET_NAME_MARKER = "-..s3BucketName..-"
    S3_KEY_MARKER = "-..s3Key..-"


class SQSLibExtended(SQS.Channel):
    """
    A session stores configuration state and allows you to create service
    clients and resources.
    :type aws_access_key_id: string
    :param aws_access_key_id: AWS access key ID
    :type aws_secret_access_key: string
    :param aws_secret_access_key: AWS secret access key
    :type aws_session_token: string
    :param aws_session_token: AWS temporary session token
    :type region_name: string
    :param region_name: Default region when creating new connections
    :type botocore_session: botocore.session.Session
    :param botocore_session: Use this Botocore session instead of creating a new default one.
    :type profile_name: string
    :param profile_name: The name of a profile to use. If not given, then the default profile is used.
    """
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, aws_region_name=None, s3_bucket_name=None):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region_name = aws_region_name
        self.s3_bucket_name = s3_bucket_name
        self.message_size_threshold = SQSExtendedClientConstants.DEFAULT_MESSAGE_SIZE_THRESHOLD
        self.always_through_s3 = False
        self.expire_days = datetime.now() + timedelta(days=14)
        
    
    def set_always_through_s3(self, always_through_s3):
        """
        Sets whether or not all messages regardless of their payload size should be stored in Amazon S3.
        """
        self.always_through_s3 = always_through_s3

    def set_message_size_threshold(self, message_size_threshold):
        """
        Sets the message size threshold for storing message payloads in Amazon S3.
        Default: 256KB.
        """
        self.message_size_threshold = message_size_threshold

    def __get_string_size_in_bytes(self, message_body):
        return len(message_body.encode('utf-8'))

    def __stringToBase64(self, s):
        return base64.b64encode(s.encode('utf-8'))

    def __base64ToString(self, b):
        return base64.b64decode(b).decode('utf-8')

    def __is_base64(self, s):
        try:
            encoded = self.__stringToBase64(self.__base64ToString(s))
            return encoded == str.encode(s)
        except Exception as e:
            return False

    def __get_msg_attributes_size(self, message_attributes):
        # sqs binaryValue expects a base64 encoded string as all messages in sqs are strings
        total_msg_attributes_size = 0

        for key, entry in message_attributes.items():
            total_msg_attributes_size += self.__get_string_size_in_bytes(key)
            if entry.get('DataType'):
                total_msg_attributes_size += self.__get_string_size_in_bytes(entry.get('DataType'))
            if entry.get('StringValue'):
                total_msg_attributes_size += self.__get_string_size_in_bytes(entry.get('StringValue'))
            if entry.get('BinaryValue'):
                if self.__is_base64(entry.get('BinaryValue')):
                    total_msg_attributes_size += len(str.encode(entry.get('BinaryValue')))
                else:
                    total_msg_attributes_size += self.__get_string_size_in_bytes(entry.get('BinaryValue'))

        return total_msg_attributes_size

    def __is_large(self, message, message_attributes):
        msg_attributes_size = self.__get_msg_attributes_size(message_attributes)
        msg_body_size = self.__get_string_size_in_bytes(message)
        total_msg_size = msg_attributes_size + msg_body_size
        return (total_msg_size > self.message_size_threshold)

    def receive_message(self, message_body):
        """
        Retrieves one or more messages (up to 10), from the specified queue. Using the WaitTimeSeconds parameter enables long-poll support
            The message body.
            An MD5 digest of the message body. For information about MD5, see RFC1321 .
            The MessageId you received when you sent the message to the queue.
            The receipt handle.
            The message attributes.
            An MD5 digest of the message attributes.
            The receipt handle is the identifier you must provide when deleting the message
        """
        if 's3BucketName' not in message_body and 's3Key' not in message_body:
            raise ValueError('Detected missing required key attribute s3BucketName and s3Key in s3 payload')
        s3_bucket_name = message_body.get('s3BucketName')
        s3_key = message_body.get('s3Key')
        orig_msg_body = self.get_text_from_S3(s3_bucket_name, s3_key)
        if orig_msg_body:
            self.__delete_message_payload_from_s3(s3_bucket_name, s3_key)
            return orig_msg_body
        return None

    def __delete_message_payload_from_s3(self,bucket_name, s3_key ):
        try:
            session = Session(aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key, region_name=self.aws_region_name)
            s3 = session.resource('s3')
            s3_object = s3.Object(bucket_name, s3_key)
            s3_object.delete()
            print('Deleted s3 object https://s3.amazonaws.com/{}/{}'.format(bucket_name, s3_key))
        except Exception as e:
            print("Failed to delete the message content in S3 object. {}, type:{}".format(
                str(e), type(e).__name__))
            raise e

    def get_message(self, message, message_attributes={}):
        """
        Delivers a message to the specified queue and uploads the message payload
        to Amazon S3 if necessary.
        """
        if message is None:
            raise ValueError('message_body required')

        msg_attributes_size = self.__get_msg_attributes_size(message_attributes)
        if msg_attributes_size > self.message_size_threshold:
            raise ValueError("Total size of Message attributes is {} bytes which is larger than the threshold of {} Bytes. Consider including the payload in the message body instead of message attributes.".format(msg_attributes_size, self.message_size_threshold))

        message_attributes_number = len(message_attributes)
        if message_attributes_number > SQSExtendedClientConstants.MAX_ALLOWED_ATTRIBUTES:
            raise ValueError("Number of message attributes [{}}] exceeds the maximum allowed for large-payload messages [{}].".format(message_attributes_number, SQSExtendedClientConstants.MAX_ALLOWED_ATTRIBUTES))

        large_payload_attribute_value = message_attributes.get(SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME)
        if large_payload_attribute_value:
            raise ValueError("Message attribute name {} is reserved for use by SQS extended client.".format(SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME))

        if self.always_through_s3 or self.__is_large(str(message), message_attributes):
            if not self.s3_bucket_name.strip():
                raise ValueError('S3 bucket name cannot be null')
            s3_key_message = json.dumps(self._store_message_in_s3(message))
            return s3_key_message
        else:
            return message

    def _store_message_in_s3(self, message_body):
        """
        Store SQS message body into user defined s3 bucket
        prerequisite aws credentials should have access to write to defined s3 bucket
        """
        try:
            s3_key = str(uuid.uuid4())
            session = Session(aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key, region_name=self.aws_region_name)
            s3 = session.resource('s3')
            s3.Bucket(self.s3_bucket_name).put_object(Key=s3_key, Body=message_body, Expires=self.expire_days)
            return {'s3BucketName': self.s3_bucket_name, 's3Key': s3_key,  "s3_refrance": True
            }
        except Exception as e:
            print("Failed to store the message content in an S3 object. SQS message was not sent. {}, type:{}".format(
                str(e), type(e).__name__))
            raise e

    def get_text_from_S3(self, s3_bucket_name, s3_key):
        """
        Get string representation of a sqs object and store into original SQS message object
        """
        
        session = Session(aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key, region_name=self.aws_region_name)
        s3 = session.resource('s3')
        try:
            obj = s3.Object(s3_bucket_name, s3_key)
            return obj.get()['Body'].read().decode('utf-8')
        except Exception as e:
            print("Failed to get the  message content in S3 object. {}, type:{}".format(
                str(e), type(e).__name__))
            raise e
        return None