#!/usr/bin/python3
# Water-Conservation-Controller-Hub.py

import sys
import socket
import selectors
import types
import pymongo
import datetime

sel = selectors.DefaultSelector()

client = pymongo.MongoClient("mongodb://localhost:27017")

def accept_wrapper(sock):
    conn, addr = sock.accept()  # Should be ready to read
    print(f"Accepted connection from {addr}")
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)
# __end_accept_wrapper__
    
def service_connection(key, mask):
    global client
    db = client['ideafest2023']
    post = {"author": "python-databse-interconnect",
            "volume": 0,
            "flow": 0,
            "tags": ["mongodb", "python", "pymongo"],
            "date": datetime.datetime.utcnow()}
    posts = db['data']
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(1024)  # Should be ready to read
        if recv_data:
            data.outb += recv_data
        else:
            print(f"Closing connection to {data.addr}")
            sel.unregister(sock)
            sock.close()
    if mask & selectors.EVENT_WRITE:
        if data.outb:
            print(f"Echoing \"{data.outb.decode('utf-8')}\" to {data.addr}")
            volume = float(str(str(data.outb, 'utf-8').split(',')[0]))
            flow = float(str(str(data.outb, 'utf-8').split(',')[1]))
            author = str(str(data.outb, 'utf-8').split(',')[2])
            post['volume'] = volume
            post['flow'] = flow
            post['author'] = author
            posts.insert_one(post).inserted_id
            sent = sock.send(data.outb)  # Should be ready to write
            data.outb = data.outb[sent:]
# __end_service_connection__

def main():
    host, port = sys.argv[1], int(sys.argv[2])
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.bind((host, port))
    lsock.listen()
    print(f"Listening on {(host, port)}")
    lsock.setblocking(False)
    sel.register(lsock, selectors.EVENT_READ, data=None)
    try:
        while True:
            events = sel.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    accept_wrapper(key.fileobj)
                else:
                    service_connection(key, mask)
    except KeyboardInterrupt:
        print("Caught keyboard interrupt, exiting")
    finally:
        sel.close()
# __end_main__


if __name__ == "__main__":
    main()
