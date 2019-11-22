import redis
import threading
import struct
import sys


channel_1 = 'publisher'
channel_tocli1 = 'sendtocli1'

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

# GANTI VARIABLE SESUAI PERINTAH (0-Packer, 1-unPacker)
def dataCorrection(text, number):
    if text == "S01":
        packerChar = 'Q h 3s h'
        unpackerChar = 'Q h 3s h 14s'
    elif text == "S03":
        packerChar = 'Q h 3s h h Q'
        unpackerChar = 'Q h 3s h Q'
    elif text == "S06":
        packerChar = 'Q h 3s h h L'
        unpackerChar = 'Q h 3s h L'
    
    if number == 1:
        return packerChar
    else:
        return unpackerChar

def commandSet(number, out):
    if number == '1':
        dataValues = (4081118888, 2, b'S01', 129)
        packer = struct.Struct('Q h 3s h')
        unpacker = struct.Struct('Q h 3s h 14s')
    elif number == '3':
        dataValues = (4081118888, 2, b'S03', 129, 0, 13999998888)
        packer = struct.Struct('Q h 3s h h Q')
        unpacker = struct.Struct('Q h 3s h Q')
    else:
        dataValues = (4081118888, 2, b'S06', 129, 0, 4000)
        packer = struct.Struct('Q h 3s h h L')
        unpacker = struct.Struct('Q h 3s h L')
    
    if out == 0:
        return dataValues
    elif out == 1:
        return packer
    elif out == 2:
        return unpacker

# ProtocolHead 1, TerminalID 10, ProtocolVersion 1, CmmndType 3
# , CmmndSN 3, Parameter(SUNNNAH) N, protocolEnd1



# RECIEVE PROCESSED DATA FROM CENTER
def recv_center(channel):
    client = redis.Redis(host='localhost', port=6379)
    p = client.pubsub()
    p.subscribe(channel)
    print(channel, " Subscribed")
    while True:
        message = p.get_message()
        if message and not message['data'] == 1:
            message = message['data']
            print(message)
            # DATA PRE-PROCESSING
            dataConverted = str(message)
            commandReceived = commandChecker(dataConverted)
            print(commandReceived)

			# UNPACKING
            unpackerChar = dataCorrection(commandReceived, 0)
            unpacker = struct.Struct(unpackerChar)
            unpackedData = unpacker.unpack(message)
            print("unpacked: ", unpackedData)
            print("Data Size: ", struct.calcsize(unpackerChar))
            
# PUBLISH TO CENTER
def publishing(channel):
    client = redis.Redis(host = 'localhost', port = 6379)
    while True:
        print("Kirim Data?")
        sendAgain = input()
        # SET THE DATA
        print("Send Data Set: ")
        out_data = input()
        dataValues = commandSet(out_data, 0)
        packer = commandSet(out_data, 1)
        # unpacker = commandSet(out_data, 2)
        packedData = packer.pack(*dataValues)

        if sendAgain == '1':
            for x in range (0, 5000):
                total = client.publish(channel, packedData)
                if total == 0:
                    print("No Subscriber")
                    break
                print(total)
                print("published: ", x)
        elif sendAgain == '0':
            break


t = threading.Thread(target=publishing, args=(channel_1,))
t.start()

t2 = threading.Thread(target=recv_center, args=(channel_tocli1,))
t2.start()


