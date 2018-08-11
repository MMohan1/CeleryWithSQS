from extend_the_kombu import *
import celeryconfig
from random import choice
from string import ascii_letters, digits
from execute_task import receive_message

def send_message():
    _1mb_large_string = ''.join([choice(ascii_letters + digits) for i in range(2048576)])
    message = _1mb_large_string
    # message = "cxjvhk" #{"sdkjfhsdjhf":"gdhgdsajdhg"}
    # message_1 = {"jashdjka":"sajkdhasjk", "asjdfh":["adsldfhasjk", "adshfjkshk"]}
    message_1 = "sdjfhjk"
    receive_message.delay(message, message_1)

send_message()