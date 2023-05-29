import socket
import threading
import sys

ENCODING = 'utf-8'
MESSAGE_LENGTH_SIZE = 64
COMMAND_LENGTH = 64
TITLE_LENGTH = 64
EXIT = False


def subscriber(title):
    global EXIT
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((sys.argv[1], int(sys.argv[2])))
        command_encoded = ('Subscribe' + ' ' * (COMMAND_LENGTH - 7)).encode(ENCODING)
        title_encoded = (title + ' ' * (TITLE_LENGTH - len(title))).encode(ENCODING)
        s.send(command_encoded)
        s.send(title_encoded)
        s.settimeout(10.0)
        try:
            sub_ack = s.recv(COMMAND_LENGTH)
            if sub_ack.decode(ENCODING).strip() == 'SubAck':
                print('subscribed on {}'.format(title))
            s.settimeout(1.0)
            while True:
                try:
                    command_decoded = s.recv(COMMAND_LENGTH).decode(ENCODING).strip()
                    if command_decoded == 'Message':
                        title_decoded = s.recv(TITLE_LENGTH).decode(ENCODING).strip()
                        message_length_decoded = int(s.recv(MESSAGE_LENGTH_SIZE).decode(ENCODING))
                        message_decoded = s.recv(message_length_decoded).decode(ENCODING)
                        print('{}: {}'.format(title_decoded, message_decoded))
                    elif command_decoded == 'Ping':
                        pong = 'Pong' + ' ' * (COMMAND_LENGTH - 4)
                        s.send(pong.encode(ENCODING))
                except socket.timeout:
                    if EXIT:
                        disconnect = 'Disconnect' + ' ' * (COMMAND_LENGTH - 10)
                        s.send(disconnect.encode(ENCODING))
                        s.close()
                        break
        except socket.timeout:
            print('subscribing on {} failed'.format(title))
    except (Exception, ) as e:
        EXIT = True
        print(e)


def publisher():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((sys.argv[1], int(sys.argv[2])))
        command = 'Publish' + ' ' * (COMMAND_LENGTH - 7)
        title = sys.argv[4] + ' ' * (TITLE_LENGTH - len(sys.argv[4]))
        tmp = str(len(sys.argv[5]))
        send_message_length = tmp + ' ' * (MESSAGE_LENGTH_SIZE - len(tmp))
        s.send(command.encode(ENCODING))
        s.send(title.encode(ENCODING))
        s.send(send_message_length.encode(ENCODING))
        s.send(sys.argv[5].encode(ENCODING))
        s.settimeout(10.0)
        try:
            pub_ack = s.recv(COMMAND_LENGTH)
            if pub_ack.decode(ENCODING).strip() == 'PubAck':
                print('your message published successfully')
        except socket.timeout:
            print('your message publishing failed')
        disconnect = 'Disconnect' + ' ' * (COMMAND_LENGTH - 10)
        s.send(disconnect.encode(ENCODING))
        s.close()
    except (Exception, ) as e:
        print(e)


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('Enter host, port and command')
    elif sys.argv[2].isnumeric():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
            result = test_socket.connect_ex((sys.argv[1], int(sys.argv[2])))
            test_socket.close()
            if not result:
                if sys.argv[3] == 'publish':
                    if len(sys.argv) < 6:
                        print('Enter topic and message')
                    else:
                        publisher()
                elif sys.argv[3] == 'subscribe':
                    if len(sys.argv) < 5:
                        print('Enter at least 1 topic')
                    else:
                        for t in sys.argv[4:]:
                            threading.Thread(target=subscriber, args=(t, )).start()
                        while not EXIT:
                            if input() == 'exit':
                                EXIT = True
                else:
                    print('The entered command is incorrect')
            else:
                print('Connection failed')
    else:
        print('The port must be numeric')
