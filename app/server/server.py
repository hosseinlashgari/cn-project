import socket
import threading
import time

HOST = '127.0.0.1'
PORT = 1379
ENCODING = 'utf-8'
MESSAGE_LENGTH_SIZE = 64
COMMAND_LENGTH = 64
TITLE_LENGTH = 64
MESSAGES = {}
SUBSCRIBERS = {}


def handler(conn, addr):
    print('Connected by {}'.format(addr))
    conn.settimeout(10.0)
    counter = 0
    connected = True
    while connected:
        try:
            data = conn.recv(COMMAND_LENGTH)
            command = data.decode(ENCODING).strip()
            if command == 'Publish':
                title = conn.recv(TITLE_LENGTH)
                title_decoded = title.decode(ENCODING).strip()
                message_length = conn.recv(MESSAGE_LENGTH_SIZE)
                message = conn.recv(int(message_length.decode(ENCODING)))
                message_decoded = message.decode(ENCODING)
                if title_decoded in MESSAGES:
                    MESSAGES[title_decoded].append(message_decoded)
                else:
                    MESSAGES[title_decoded] = [message_decoded]
                pub_ack = 'PubAck' + ' ' * (COMMAND_LENGTH - 6)
                conn.send(pub_ack.encode(ENCODING))

                send_command = 'Message' + ' ' * (COMMAND_LENGTH - 7)
                if title_decoded in SUBSCRIBERS:
                    for subscriber in SUBSCRIBERS[title_decoded]:
                        subscriber.send(send_command.encode(ENCODING))
                        subscriber.send(title)
                        subscriber.send(message_length)
                        subscriber.send(message)
                print('<{}: {}> published by {}'.format(title_decoded, message_decoded, addr))

            elif command == 'Subscribe':
                time.sleep(0.1)
                title = conn.recv(TITLE_LENGTH)
                title_decoded = title.decode(ENCODING).strip()
                if title_decoded in SUBSCRIBERS:
                    SUBSCRIBERS[title_decoded].append(conn)
                else:
                    SUBSCRIBERS[title_decoded] = [conn]
                sub_ack = 'SubAck' + ' ' * (COMMAND_LENGTH - 6)
                conn.send(sub_ack.encode(ENCODING))

                send_command = 'Message' + ' ' * (COMMAND_LENGTH - 7)
                if title_decoded in MESSAGES:
                    for message in MESSAGES[title_decoded]:
                        tmp = str(len(message))
                        send_message_length = tmp + ' ' * (MESSAGE_LENGTH_SIZE - len(tmp))
                        conn.send(send_command.encode(ENCODING))
                        conn.send(title)
                        conn.send(send_message_length.encode(ENCODING))
                        conn.send(message.encode(ENCODING))
                print('{} subscribed on {}'.format(addr, title_decoded))

            elif command == 'Disconnect':
                for title in SUBSCRIBERS:
                    if conn in SUBSCRIBERS[title]:
                        SUBSCRIBERS[title].remove(conn)
                conn.close()
                connected = False
                print('Disconnected by {}'.format(addr))
            elif command == 'Pong':
                counter = 0
            elif command == 'Ping':
                pong = 'Pong' + ' ' * (COMMAND_LENGTH - 4)
                conn.send(pong.encode(ENCODING))
        except socket.timeout:
            if counter == 3:
                for title in SUBSCRIBERS:
                    if conn in SUBSCRIBERS[title]:
                        SUBSCRIBERS[title].remove(conn)
                conn.close()
                connected = False
                print('{} disconnected by Server'.format(addr))
            else:
                if counter > 0:
                    print('{} did not answer. [{}]'.format(addr, counter))
                ping = 'Ping' + ' ' * (COMMAND_LENGTH - 4)
                try:
                    conn.send(ping.encode(ENCODING))
                except (Exception, ) as e:
                    print('{}: {}'.format(addr, e))
                    conn.close()
                    connected = False
                counter += 1
        except (Exception, ) as e:
            print('{}: {}'.format(addr, e))
            conn.close()
            connected = False


if __name__ == '__main__':
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        while True:
            connection, address = s.accept()
            threading.Thread(target=handler, args=(connection, address)).start()
