import redis
import threading
import time
import struct
import sys


channel_2 = 'toprocessor'
channel_center = 'recognizing'
channel_toCenter = 'sendtocenter'
toSendBack = []


def callData_fromCenter(channel):
	client = redis.Redis(host = 'localhost', port = 6379)
	# ENTRY NEW PROCESSOR, NEW CHANNEL
	while True:
		print("Masukkan Nama Channel")
		channelName = input()
		total = client.publish(channel, channelName)
		if total == 0:
			print("no subscriber")
			print("--RETRY--")
		else:
			print("CHANNEL SUBSCRIBED")
			break
	
	# START NEW THREAD
	t_start = threading.Thread(target=fromCenter, args=(channelName+".pub",))
	t_start.start()

	# CALL DATA
	while True:
		print("Panggil Data?")
		callTrue = input()
		if callTrue == '0':
			print("--SEND UNSUBSCRIBE TO CENTER--")
			while True:
				total = client.publish(channelName, '0')
				if total == 0:
					print(total, "SUBSCRIBER IN ", channelName, "--THREAD DONE")
					break
			break
			# BREAK THE THREAD

		else:
			total = client.publish(channelName, '1')
			print("DATA CALLED")

			
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


# MEMBUAT DATA UNTUK DIKIRIM ULANG
def generateSendData(text):
    print(text[2])
    if text[2] == b'S01':
        values = (text[0], text[1], text[2], text[3], b'www.lalala.com')
    elif text[2] == b'S03':
        values = (text[0], text[1], text[2], text[3], text[5])
    elif text[2] == b'S06':
        values = (text[0], text[1], text[2], text[3], text[5])
    return values       

# GANTI VARIABLE SESUAI PERINTAH (0-Packer, 1-unPacker)
def dataCorrection(text, number):
    if text == "S01":
        unpackerChar = 'Q h 3s h'
        packerChar = 'Q h 3s h 14s'
    elif text == "S03":
        unpackerChar = 'Q h 3s h h Q'
        packerChar = 'Q h 3s h Q'
    elif text == "S06":
        unpackerChar = 'Q h 3s h h L'
        packerChar = 'Q h 3s h L'
    
    if number == 1:
        return packerChar
    else:
        return unpackerChar
    

def send_toCenter(channel):
	client = redis.Redis(host='localhost', port=6379)
	while True:
		dataLeft = len(toSendBack)
		if dataLeft > 0:
			toPop = toSendBack.pop(0)
			print("Send Back This Data: ", toPop)
			client.publish(channel, toPop)
			print(len(toSendBack), " -- Data Left")

# MENERIMA DATA DARI CENTER
def fromCenter(channel):
	client = redis.Redis(host='localhost', port=6379)
	p = client.pubsub()
	p.subscribe(channel)
	print(channel, " Subscribed")
	while True:
		message = p.get_message()
		if message and not message['data'] == 1:
			message = message['data']

			# DATA PRE-PROCESSING
			dataConverted = str(message)
			commandReceived = commandChecker(dataConverted)
			print(commandReceived)

			# UNPACKING
			unpackerChar = dataCorrection(commandReceived, 0)
			unpacker = struct.Struct(unpackerChar)
			unpackedData = unpacker.unpack(message)
			print("unpacked: ", unpackedData)
			print(struct.calcsize(unpackerChar))

			 # GENERATE DATA TO SENDBACK
			SendBack = generateSendData(unpackedData)
			print("data to send: ", SendBack)
            
			packerChar = dataCorrection(commandReceived, 1)
			packer = struct.Struct(packerChar)
			packedSendBack = packer.pack(*SendBack)
			
			toSendBack.append(packedSendBack)
			print(packedSendBack)
			print(len(toSendBack))

t1 = threading.Thread(target=callData_fromCenter, args=(channel_center,))
t1.start()

t2 = threading.Thread(target=send_toCenter, args=(channel_toCenter,))
t2.start()