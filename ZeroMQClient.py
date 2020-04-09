
import tornado.ioloop, tornado.web, tornado.websocket, os.path
from tornado import web
from datetime import datetime
import json
import time
import paramiko
import sys
import select
import os
import cx_Oracle
import threading

# in windows kalau error nomodule terimos edit file ini C:\Python27\Lib\tty.py uncomment semuanya
import spur
import zmq
from zmq.eventloop import ioloop, zmqstream
ioloop.install()

#period = 2 * 60 * 1000   # every 2 minutes
#period = 1 * 60 * 1000   # every 1 minutes
period = 0.5 * 60 * 1000   # every 1 minutes

#the width of the display
#(the windows console is 79 characters wide).
#WIDTH = 79
WIDTH = 50
#the message we wish to print
text = "HELLO!".upper()
#the printed banner version of the message
#this is a 7-line display, stored as 7 strings
#initially, these are empty.
printedMessage = [ "","","","","","","" ]
#a dictionary mapping letters to their 7-line
#banner display equivalents. each letter in the dictionary
#maps to 7 strings, one for each line of the display.
characters = { " " : [ " ",
                       " ",
                       " ",
                       " ",
                       " ",
                       " ",
                       " " ],

               "E" : [ "*****",
                       "*    ",
                       "*    ",
                       "*****",
                       "*    ",
                       "*    ",
                       "*****" ],
               
               "H" : [ "*   *",
                       "*   *",
                       "*   *",
                       "*****",
                       "*   *",
                       "*   *",
                       "*   *" ], 

               "O" : [ "*****",
                       "*   *",
                       "*   *",
                       "*   *",
                       "*   *",
                       "*   *",
                       "*****" ],

               "L" : [ "*    ",
                       "*    ",
                       "*    ",
                       "*    ",
                       "*    ",
                       "*    ",
                       "*****" ],

               "!" : [ "  *  ",
                       "  *  ",
                       "  *  ",
                       "  *  ",
                       "  *  ",
                       "     ",
                       "  *  " ]               
               }

ip_ZeroMQserver     = sys.argv[1]
ip_database         = sys.argv[2]

port          = 1521
SID           = 'XE'
dsn_tns       = cx_Oracle.makedsn(ip_database, port, SID)
DBPool        = cx_Oracle.SessionPool(user='IRS',password='IRS041972', dsn=dsn_tns, min=1, max=2, increment=1) 
DBPool.timeout= 120 #idle session timeout
connection    = DBPool.acquire()

def dot():
    """callback for showing that IOLoop is still responsive while we wait"""
    #sys.stdout.write('>')
    #sys.stdout.flush()
    for row in range(7):
        for char in text:
            printedMessage[row] += (str(characters[char][row]) + "  ")

    #the offset is how far to the right we want to print the message.
    #initially, we want to print the message just off the display.
    offset = 10
    #while True:
    os.system("cls")
    #print each line of the message, including the offset.
    for row in range(7):
        print(" " * offset + printedMessage[row][max(0,offset*-1):WIDTH - offset])
    #move the message a little to the left.
    offset -=1
    #if the entire message has moved 'through' the display then
    #start again from the right hand side.
    if offset <= ((len(text)+2)*6) * -1:
        offset = WIDTH
    #take out or change this line to speed up / slow down the display
    time.sleep(0.05)    

    print ""
    print "[ Status           : Idle ....]"
    print "[ File             : ZeroMQClient ]" 
    print "[ IP ZeroMQServer  : "+ip_ZeroMQserver+"] " 
    print "[ IP Database      : "+ip_database+"] " 


def printer(msg):
    print (msg)

def check_None_string(value):
    if value is None:
        return_value = ""
    else :    
        return_value = value

    return return_value

def check_None_number(value):
    if value is None:
        return_value = "NULL"
    else :    
        return_value = value

    return return_value

def ITEM_MASTER():

    ## ZeroMQ Client
    context    = zmq.Context(1)
    socket     = context.socket(zmq.SUB)
    subscriber = zmqstream.ZMQStream(socket)
    socket.setsockopt(zmq.SUBSCRIBE, "ITEM_MASTER")
    #socket.connect('tcp://*:5555')    
    #socket.connect("tcp://127.0.0.1:5562")
    socket.connect("tcp://"+ip_ZeroMQserver+":5562")

    def subscription(message):

        if len(message[1])>2:

            print "ITEM_MASTER  %s" % (message[1])
            result = json.loads(message[1])
            # create connection 
            cursor = connection.cursor()

            # Looping form json 
            for row in result:
                # Initiate variable 
                plu                     = check_None_string(row["PLU"])
                plu_external            = check_None_string(row["PLU_EXTERNAL"])
                brand                   = check_None_string(row["BRAND"])                    
                weighing                = check_None_string(row["WEIGHING"])
                purchase_method         = check_None_string(row["PURCHASE_METHOD"])
                vendor                  = check_None_string(row["VENDOR"])
                subdivision             = check_None_string(row["SUBDIVISION"])
                last_purch_cost         = check_None_number(row["LAST_PURCH_COST"])
                last_purch_cost_uom     = check_None_string(row["LAST_PURCH_COST_UOM"])
                last_rec_cost           = check_None_number(row["LAST_REC_COST"])
                last_rec_cost_uom       = check_None_string(row["LAST_REC_COST_UOM"])
                avg_cost                = check_None_number(row["AVG_COST"])
                last_purch_cost_b4ppn   = check_None_number(row["LAST_PURCH_COST_B4PPN"])
                last_purch_cost_uom_b4ppn = check_None_string(row["LAST_PURCH_COST_UOM_B4PPN"])
                plu_type                    = check_None_string(row["PLU_TYPE"])
                plu_active                  = check_None_string(row["PLU_ACTIVE"])


                # check item_master 
                sql = '''SELECT 1 FROM Item_mst WHERE PLU = '%s'  '''  %(row["PLU"] )                          
                cursor.execute(sql)
                cursor.fetchone()

                if not cursor.rowcount:
                    #print "No results found"
                    sql = '''
                          INSERT INTO item_mst(
                                  plu,  plu_external, brand, weighing, purchase_method, vendor,subdivision,
                                  last_purch_cost, last_purch_cost_uom,
                                  last_rec_cost, last_rec_cost_uom, avg_cost,
                                  last_purch_cost_b4ppn,last_purch_cost_uom_b4ppn,
                                  plu_type, plu_active  
                                  ) 
                            VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s',
                                    %s,  '%s',
                                    %s,  '%s',  %s, 
                                    %s,  '%s',
                                   '%s', '%s'
                                   )
                          ''' % ( plu,  plu_external, brand.replace("'",""), weighing, purchase_method, vendor,subdivision,
                                  last_purch_cost, last_purch_cost_uom,
                                  last_rec_cost, last_rec_cost_uom, avg_cost,
                                  last_purch_cost_b4ppn,last_purch_cost_uom_b4ppn,
                                  plu_type, plu_active  
                                )

                    #print sql
                    cursor.execute(sql)
                    #print "Insert Article"

                else:
                    #print "results found"
                    sql =   ''' 
                            UPDATE item_mst
                               SET  plu_external            = '%s', 
                                    brand                   = '%s', 
                                    weighing                = '%s',  
                                    purchase_method         = '%s', 
                                    vendor                  = '%s', 
                                    subdivision             = '%s', 
                                    last_purch_cost         =  %s,  
                                    last_purch_cost_uom     = '%s', 
                                    last_rec_cost           = %s, 
                                    last_rec_cost_uom       = '%s', 
                                    avg_cost                = %s, 
                                    last_purch_cost_b4ppn   = %s, 
                                    last_purch_cost_uom_b4ppn = '%s', 
                                    plu_type                  = '%s',   
                                    plu_active                = '%s'     
                              WHERE plu = '%s' 
                              ''' %(plu_external,
                                    brand.replace("'",""),
                                    weighing,
                                    purchase_method,
                                    vendor,
                                    subdivision,
                                    last_purch_cost,
                                    last_purch_cost_uom,
                                    last_rec_cost,
                                    last_rec_cost_uom,
                                    avg_cost,
                                    last_purch_cost_b4ppn,
                                    last_purch_cost_uom_b4ppn,
                                    plu_type ,
                                    plu_active,
                                    plu
                                  )
   
                    #print sql
                    cursor.execute(sql)
                    #print "Update Article"

                connection.commit()
                #cursor.close()

    subscriber.on_recv(subscription)
    #ioloop.IOLoop.instance().start()
    dot()

def POS_PLU_ALT():

    ## ZeroMQ Client
    context    = zmq.Context(1)
    socket     = context.socket(zmq.SUB)
    subscriber = zmqstream.ZMQStream(socket)
    socket.setsockopt(zmq.SUBSCRIBE, "POS_PLU_ALT")
    #socket.connect('tcp://*:5555')
    #socket.connect("tcp://127.0.0.1:5562")
    socket.connect("tcp://"+ip_ZeroMQserver+":5562")

    def subscription(message):

        if len(message[1])>2:
            print "POS_PLU_ALT : %s" % (message[1])

            result = json.loads(message[1])
            # create connection 
            cursor = connection.cursor()

            # Looping form json 
            for row in result:
                # Initiate variable 
                PLU                = check_None_string(row["PLU"])
                PLU_ALT            = check_None_string(row["PLU_ALT"])
                QTY_UOM            = check_None_string(row["QTY_UOM"])                    
                KETERANGAN         = check_None_string(row["KETERANGAN"])

                # check item_master 
                sql = '''SELECT 1 FROM POS_PLU_ALT WHERE PLU_ALT = '%s'  '''  %(PLU_ALT)                          
                cursor.execute(sql)
                cursor.fetchone()
                if not cursor.rowcount:
                    #print "No results found"
                    sql = '''
                          INSERT INTO POS_PLU_ALT(PLU,  PLU_ALT, QTY_UOM, KETERANGAN) 
                                          VALUES('%s', '%s', '%s', '%s')
                          ''' % ( PLU,  PLU_ALT, QTY_UOM, KETERANGAN.replace("'",""))

                    #print sql
                    cursor.execute(sql)
                    #print "Insert Article"

                else:
                    #print "results found"
                    sql =   ''' 
                            UPDATE POS_PLU_ALT
                               SET  PLU            = '%s',                                
                                    QTY_UOM        = '%s', 
                                    KETERANGAN     = '%s'
                              WHERE PLU_ALT = '%s' 
                              ''' %(PLU,
                                    QTY_UOM,
                                    KETERANGAN.replace("'",""),
                                    PLU_ALT
                                  )
   
                    #print sql
                    cursor.execute(sql)
                    #print "Update Article"

                connection.commit()
                #cursor.close()

    subscriber.on_recv(subscription)
    #ioloop.IOLoop.instance().start()
    dot()

def  PC_SALES():

    ## ZeroMQ Client
    context    = zmq.Context(1)
    socket     = context.socket(zmq.SUB)
    subscriber = zmqstream.ZMQStream(socket)
    socket.setsockopt(zmq.SUBSCRIBE, "PC_SALES")
    #socket.connect('tcp://*:5555')
    #socket.connect("tcp://127.0.0.1:5562")
    socket.connect("tcp://"+ip_ZeroMQserver+":5562")

    def subscription(message):
        if len(message[1])>2:
            print "PC_SALES : %s" % (message[1])

            result = json.loads(message[1])
            # create connection 
            cursor = connection.cursor()

            # Looping form json 
            for row in result:
                # Initiate variable 
                PC_SEQ                = check_None_number(row["PC_SEQ"])
                PLU                   = check_None_string(row["PLU"])
                PLU_ALT               = check_None_string(row["PLU_ALT"])                    
                UNIT_JUAL             = check_None_string(row["UNIT_JUAL"])
                NEW_RETAIL_PRICE      = check_None_number(row["NEW_RETAIL_PRICE"])
                NEW_RETAIL_PRICE_UOM  = check_None_string(row["NEW_RETAIL_PRICE_UOM"])
                MARGIN                = check_None_number(row["MARGIN"])
                START_DATE            = check_None_number(row["START_DATE"])
                END_DATE              = check_None_number(row["END_DATE"])

                # check item_master 
                sql = '''SELECT 1 FROM PC_SALES WHERE PC_SEQ = %s  '''  %(row["PC_SEQ"] )                          
                cursor.execute(sql)
                cursor.fetchone()
                if not cursor.rowcount:
                    #print "No results found"
                    sql = '''
                          INSERT INTO PC_SALES(PC_SEQ, PLU, PLU_ALT,  UNIT_JUAL, NEW_RETAIL_PRICE, NEW_RETAIL_PRICE_UOM,  MARGIN, START_DATE, END_DATE) 
                                          VALUES(%s, '%s', '%s', '%s',  %s, '%s', %s,TO_DATE('%s','DDMMYYYY'), TO_DATE('%s','DDMMYYYY') ) 
                          ''' % ( PC_SEQ, PLU, PLU_ALT,  UNIT_JUAL, NEW_RETAIL_PRICE, NEW_RETAIL_PRICE_UOM,  MARGIN, START_DATE, END_DATE )

                    #print sql
                    cursor.execute(sql)
                    #print "Insert Article"

                else:
                    #print "results found"
                    sql =   ''' 
                            UPDATE PC_SALES
                               SET  PLU               = '%s', 
                                    PLU_ALT           = '%s', 
                                    UNIT_JUAL         = '%s',
                                    NEW_RETAIL_PRICE  =  %s,
                                    NEW_RETAIL_PRICE_UOM  = '%s',
                                    MARGIN                = %s,
                                    START_DATE            = TO_DATE('%s','DDMMYYYY'),
                                    END_DATE              = TO_DATE('%s','DDMMYYYY')
                              WHERE PC_SEQ = %s
                              ''' %(PLU,
                                    PLU_ALT,
                                    UNIT_JUAL,
                                    NEW_RETAIL_PRICE,
                                    NEW_RETAIL_PRICE_UOM,
                                    MARGIN,
                                    START_DATE,
                                    END_DATE,
                                    PC_SEQ
                                  )
   
                    #print sql
                    cursor.execute(sql)
                    #print "Update Article"

                connection.commit()
                #cursor.close()

    subscriber.on_recv(subscription)
    #ioloop.IOLoop.instance().start()
    dot()

# Sample def
def publish():
  publisher.send_multipart(("heartbeat", "OHAI"))

def main():
    try:
        #ioloop.PeriodicCallback(dot, period).start()
        #beat = ioloop.PeriodicCallback(dot, period)
        #beat.start()    

        # Note that code is executed sequantially!
        # blocking 
        #ioloop.IOLoop.instance().add_callback(ITEM_MASTER)
        #ioloop.IOLoop.instance().add_callback(POS_PLU_ALT)
        #ioloop.IOLoop.instance().add_callback(PC_SALES)
        #ioloop.IOLoop.instance().start()

        # non blocking 
        #ioloop.PeriodicCallback(dot, period).start()
        ioloop.PeriodicCallback(ITEM_MASTER, period).start()
        ioloop.PeriodicCallback(POS_PLU_ALT, period).start()
        ioloop.PeriodicCallback(PC_SALES, period).start()
        ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        print('Exit')

if __name__ == "__main__":
    main()

# run auto reload 
# python -m tornado.autoreload IRSZeroMQClient.py
