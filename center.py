import redis
import threading
import struct
import sys

channel_1 = 'publisher'

channel_2 = 'toprocessor'
channel_center = 'recognizing'

channel_toCenter = 'sendtocenter'
channel_tocli1 = 'sendtocli1'


test1Data = []
toClient1_data = []

def send_toClient1(channel):
	client = redis.Redis(host='localhost', port=6379)
	while True:
		dataLeft = len(toClient1_data)
		if dataLeft > 0:
			toPop = toClient1_data.pop(0)
			print("Send Back This Data: ", toPop)
			client.publish(channel, toPop)
			print(len(toClient1_data), " -- Data Left")

# RECEIVE PROCESSED DATA FROM CLIENT 2
def recv_client2(channel):
	client = redis.Redis(host='localhost', port=6379)
	p = client.pubsub()
	p.subscribe(channel)
	print(channel, " Subscribed")
	while True:
		message = p.get_message()
		if message and not message['data'] == 1:
			message = message['data']
			print(message)
			toClient1_data.append(message)

# CHANNEL CORRECTION TO CLIENT 2
def channelCorrection(channel_list, toFind):
	totalChannel = len(channel_list)
	for i in range (0, totalChannel):
		toCheck = channel_list[i].decode('UTF-8')
		if toCheck == toFind:
			return 1
	return 0

# RECONIZING CLIENT 2
def toClient_2_Recognize(channel):
	client = redis.Redis(host='localhost', port = 6379)
	p = client.pubsub()
	p.subscribe(channel)
	print(channel, " Subscribed")
	while True:
		message = p.get_message()
		if message and not message['data'] == 1:
			message = message['data']
			message = message.decode('UTF-8')
			t_new = threading.Thread(target=toClient2, args=(message,))
			t_new.start()
			print(message, " -- ready to use")
			

# FROM CENTER TO CLIENT 2
def toClient2(channel):
	client = redis.Redis(host='localhost', port = 6379)
	p = client.pubsub()
	p.subscribe(channel)
	print(channel, " Subscribed")
	while True:
		message = p.get_message()
		if message and not message['data'] == 1:
			message = message['data']
			message = message.decode('UTF-8')
			if message == '0':
				p.unsubscribe(channel)
				print(channel, " -- UNSUBSCRIBED")
				break
			else:
				toPop = test1Data.pop(0)
				client.publish(channel+".pub", toPop)
				print("Message Published")
				print(len(test1Data))

# CEK PERINTAH YANG DITERIMA
def commandChecker(text):
    commandCheck = ''
    notedNumber = 0
    for xturn in range (0, len(text)):
        if text[xturn] == 'S':
            notedNumber = xturn
            commandCheck = text[notedNumber] + text[notedNumber+1] + text[notedNumber+2]
            break
    return commandCheck

# FROM CLIENT 1 TO CENTER
def fromClient_1(channel):
	client = redis.Redis(host = 'localhost', port = 6379)
	p = client.pubsub()
	p.subscribe(channel)
	print(channel, " Subscribed")
	while True:
		message = p.get_message()
		if message and not message['data'] == 1:
			message = message['data']
			print(message)
			
			test1Data.append(message)
			print(len(test1Data))

t1 = threading.Thread(target=fromClient_1, args=(channel_1,))
t1.start()

t2 = threading.Thread(target=toClient_2_Recognize, args=(channel_center,))
t2.start()

t3 = threading.Thread(target=recv_client2, args=(channel_toCenter,))
t3.start()

t4 = threading.Thread(target=send_toClient1, args=(channel_tocli1,))
t4.start()

