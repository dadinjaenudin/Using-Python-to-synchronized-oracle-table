
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

## ZeroMQ Server
context = zmq.Context()

# Subscriber tells us when it's ready here
receiver = context.socket(zmq.PULL)
receiver.bind("tcp://*:5561")
#receiver.setsockopt(zmq.RCVBUF, 256)
#receiver.setsockopt(zmq.RCVHWM, 100)  # Receiving every 1000

# We send updates via this socket
publisher = context.socket(zmq.PUB)
publisher.setsockopt(zmq.LINGER, 0)
publisher.bind("tcp://*:5562")

#publisher.setsockopt(zmq.SNDHWM, 1)
#receiver.setsockopt(zmq.RCVHWM, 1)
#publisher.setsockopt(zmq.SNDBUF, 256)
#receiver.setsockopt(zmq.RCVBUF, 256)

#period = 2 * 60 * 1000   # every 2 minutes
# for initial
# period = 1 * 60 * 1000   # every 1 minutes

# for realtime
period = 0.5 * 60 * 1000   # every 1 minutes

#the width of the display
#(the windows console is 79 characters wide).
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

def set_text_color(color):
    #This method works on windows only
    color_codes = {"BLUE" : 0x0001, "GREEN" : 0x0002, "RED" : 0x0004, "MAGENTA" : 0x0005, "YELLOW" : 0x0006, "GRAY" : 0x0007}
    STD_OUTPUT_HANDLE = -11
    from ctypes import windll, Structure, c_short, c_ushort, byref
    stdout_handle = windll.kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
    SetConsoleTextAttribute = windll.kernel32.SetConsoleTextAttribute
    SetConsoleTextAttribute(stdout_handle, color_codes[color])


# Python ip_ZeroMQserver ip_database [1=initial]/[2=daily]/[3=realtime]
# initial = all
ip_ZeroMQserver     = sys.argv[1]
ip_database         = sys.argv[2]

port          = 1521
SID           = 'XE'
dsn_tns       = cx_Oracle.makedsn(ip_database, port, SID)
DBPool        = cx_Oracle.SessionPool(user='irs',password='irs041972', dsn=dsn_tns, min=1, max=2, increment=1) 
DBPool.timeout= 120 #idle session timeout
connection    = DBPool.acquire()
#db2 = cx_Oracle.connect('hr', 'hrpwd', dsn_tns)


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

    #set_text_color("GREEN")
    #set_text_color("YELLOW")
    #set_text_color("RED")
    #set_text_color("GREY")
    set_text_color("MAGENTA")

    print ""
    print "[ Status           : Idle ....]"
    print "[ File             : ZeroMQServer ]" 
    print "[ IP ZeroMQServer  : "+ip_ZeroMQserver+"] " 
    print "[ IP Database      : "+ip_database+"] " 

def MASTER_REALTIME():

        os.system("COLOR 0A")
        os.system("cls")
        #dot()
        #------------------------------------------------------------------------------#
        # PLEASE NOTE : SEMUA PERUBAHGAN DATA TRIGGER ADA DI ITEM_MASTER 
        #------------------------------------------------------------------------------#

        # date string to datetime object
        date_str = "2008-11-10 17:53:59"
        cursor = connection.cursor()

        #------------------------------------------------------------------------------#
        # Check last updated from pos_mechine
        #------------------------------------------------------------------------------#    
        #v_mechine_no = "KASSA1"
        query = ''' SELECT DECODE(last_update,NULL,SYSDATE,last_update) 
                      FROM POS_PARAMETER 
                      WHERE rownum = 1
                ''' 
        cursor.execute(query)
        query_result = [ dict(line) for line in [zip([ column[0] for column in 
                             cursor.description], row) for row in cursor.fetchall()] ]     

        d_last_update = row[0].strftime('%d/%m/%Y %H:%M:%S') 
        #print d_last_update
            
        #------------------------------------------------------------------------------#
        # Query Uom Master 
        #------------------------------------------------------------------------------#
        query = ''' 
                  SELECT UOM, UOM_NAME, WEIGHING, UOM_VALUE
                    FROM UOM_MST
                   WHERE CREATED >= TO_DATE('%s','DD/MM/YYYY HH24:MI:SS') 
                '''  %(d_last_update)
                
        cursor.execute(query)
        query_result = [ dict(line) for line in [zip([ column[0] for column in 
                             cursor.description], row) for row in cursor.fetchall()] ]     
        #json_encoded = json.dumps(query_result)
        json_encoded = json.dumps(query_result, indent=4)
        publisher.send_multipart(("UOM_MST", json_encoded))
        #if len(json_encoded)<=2:
        #    dot()

        if len(json_encoded)>2:
            print "UOM_MST :: %s" % (json_encoded)

        #------------------------------------------------------------------------------#
        # Query Uom Conversion Master 
        #------------------------------------------------------------------------------#
        query = ''' 
                  SELECT UOM_FROM, UOM_TO, UOM_FACTOR
                    FROM UOM_CONV_MST
                   WHERE CREATED >= TO_DATE('%s','DD/MM/YYYY HH24:MI:SS') 
                '''  %(d_last_update)
                
        cursor.execute(query)
        query_result = [ dict(line) for line in [zip([ column[0] for column in 
                             cursor.description], row) for row in cursor.fetchall()] ]     
        #json_encoded = json.dumps(query_result)
        json_encoded = json.dumps(query_result, indent=4)
        publisher.send_multipart(("UOM_CONV_MST", json_encoded))
        #if len(json_encoded)<=2:
        #    dot()

        if len(json_encoded)>2:
            print "UOM_CONV_MST :: %s" % (json_encoded)

        #------------------------------------------------------------------------------#
        # Vendor MASTER
        #------------------------------------------------------------------------------#
        query = ''' 
                  SELECT VENDOR, NAME, ADDR
                    FROM VENDOR_MST
                   WHERE CREATE_DATE >= TO_DATE('%s','DD/MM/YYYY HH24:MI:SS') 
                '''  %(d_last_update)
                
        cursor.execute(query)
        query_result = [ dict(line) for line in [zip([ column[0] for column in 
                             cursor.description], row) for row in cursor.fetchall()] ]     
        #json_encoded = json.dumps(query_result)
        json_encoded = json.dumps(query_result, indent=4)
        publisher.send_multipart(("VENDOR_MST", json_encoded))
        #if len(json_encoded)<=2:
        #    dot()

        if len(json_encoded)>2:
            print "VENDOR_MST :: %s" % (json_encoded)

        #------------------------------------------------------------------------------#
        # USER_MST
        #------------------------------------------------------------------------------#
        query = ''' 
                  SELECT USERNAME, PASSWORD, PROFILE, USER_ID, ACTIVE, DELETE_DATE, CREATE_DATE, USER_DB, USER_POS, MENUPROFILE
                    FROM USER_MST
                   WHERE CREATE_DATE >= TO_DATE('%s','DD/MM/YYYY HH24:MI:SS') 
                '''  %(d_last_update)
                
        cursor.execute(query)
        query_result = [ dict(line) for line in [zip([ column[0] for column in 
                             cursor.description], row) for row in cursor.fetchall()] ]     
        #json_encoded = json.dumps(query_result)
        json_encoded = json.dumps(query_result, indent=4)
        publisher.send_multipart(("USER_MST", json_encoded))
        #if len(json_encoded)<=2:
        #    dot()

        if len(json_encoded)>2:
            print "USER_MST :: %s" % (json_encoded)

        #------------------------------------------------------------------------------#
        # POS_USER
        #------------------------------------------------------------------------------#
        query = ''' 
                  SELECT USER_ID, USER_NAME, PASSWD
                    FROM POS_USER
                '''
        cursor.execute(query)
        query_result = [ dict(line) for line in [zip([ column[0] for column in 
                             cursor.description], row) for row in cursor.fetchall()] ]     
        #json_encoded = json.dumps(query_result)
        json_encoded = json.dumps(query_result, indent=4)
        publisher.send_multipart(("POS_USER", json_encoded))
        #if len(json_encoded)<=2:
        #    dot()
        if len(json_encoded)>2:
            print "POS_USER :: %s" % (json_encoded)

        #------------------------------------------------------------------------------#
        # Query Item Master filter with timestamp
        # Note : Any changed in item_mst will execute posplu_alt, pc_sales
        #------------------------------------------------------------------------------#
        query = ''' 
                SELECT  a.plu,  
                        a.plu_external,
                        a.brand, 
                        a.weighing,  
                        a.purchase_method,
                        a.vendor, 
                        a.subdivision, 
                        a.last_purch_cost, 
                        a.last_purch_cost_uom, 
                        a.last_rec_cost, 
                        a.last_rec_cost_uom, 
                        a.avg_cost, 
                        a.last_purch_cost_b4ppn, 
                        a.last_purch_cost_uom_b4ppn,
                        a.plu_type ,
                        a.plu_active
                    FROM item_mst a                 
                   WHERE CREATE_DATE >= TO_DATE('%s','DD/MM/YYYY HH24:MI:SS') 
                '''  %(d_last_update)
                
        cursor.execute(query)
        query_result = [ dict(line) for line in [zip([ column[0] for column in 
                             cursor.description], row) for row in cursor.fetchall()] ]     
        
        json_encoded = json.dumps(query_result)
        #json_encoded = json.dumps(query_result, indent=4)
        publisher.send_multipart(("ITEM_MASTER", json_encoded))
        #if len(json_encoded)<=2:
        #    dot()

        if len(json_encoded)>2:
              print "ITEM_MASTER :: %s" % (json_encoded)

              #------------------------------------------------------------------------------#
              # Query POSPLU_ALT Master 
              #------------------------------------------------------------------------------#
              query = ''' 
                      SELECT  PLU, PLU_ALT, QTY_UOM, KETERANGAN
                        FROM POS_PLU_ALT                 
                       WHERE PLU IN ( SELECT PLU FROM ITEM_MST WHERE CREATE_DATE >= TO_DATE('%s','DD/MM/YYYY HH24:MI:SS') )
                      '''  %(d_last_update)
                      
              cursor.execute(query)
              query_result = [ dict(line) for line in [zip([ column[0] for column in 
                                   cursor.description], row) for row in cursor.fetchall()] ]     
              #json_encoded = json.dumps(query_result)
              json_encoded = json.dumps(query_result, indent=4)
              publisher.send_multipart(("POS_PLU_ALT", json_encoded))
              #if len(json_encoded)<=2:
              #    dot()

              if len(json_encoded)>2:
                  print "POS_PLU_ALT :: %s" % (json_encoded)

              #------------------------------------------------------------------------------#
              # Query POSPLU_ALT Master 
              #------------------------------------------------------------------------------#
              query = ''' 
                      SELECT  PC_SEQ, PLU, PLU_ALT,  
                              UNIT_JUAL, 
                              NEW_RETAIL_PRICE, NEW_RETAIL_PRICE_UOM,  
                              NEW_RETAIL_PRICE2,
                              NEW_RETAIL_PRICE3,
                              NEW_RETAIL_PRICE4,
                              MARGIN, 
                              MARGIN2, 
                              MARGIN3, 
                              MARGIN4, 
                              TO_CHAR(START_DATE,'DDMMYYYY')  START_DATE, 
                              TO_CHAR(END_DATE,'DDMMYYYY')    END_DATE
                        FROM PC_SALES                 
                       WHERE PLU IN ( SELECT PLU FROM ITEM_MST WHERE CREATE_DATE >= TO_DATE('%s','DD/MM/YYYY HH24:MI:SS') )
                      '''  %(d_last_update)
                      
              cursor.execute(query)
              query_result = [ dict(line) for line in [zip([ column[0] for column in 
                                   cursor.description], row) for row in cursor.fetchall()] ]     
              #json_encoded = json.dumps(query_result)
              json_encoded = json.dumps(query_result, indent=4)
              publisher.send_multipart(("PC_SALES", json_encoded))
              #if len(json_encoded)<=2:
              #    dot()

              if len(json_encoded)>2:
                  print "PC_SALES :: %s" % (json_encoded)

        #------------------------------------------------------------------------------#
        # Update timestamp  pos_parameter
        #------------------------------------------------------------------------------#
        i = datetime.now()
        d_time_stamp = i.strftime('%d/%m/%Y %H:%M:%S')    
        #print d_time_stamp
        query = "UPDATE POS_PARAMETER \
                    SET last_update = TO_DATE('%s','DD/MM/YYYY HH24:MI:SS') \
                  WHERE rownum=1 "   %(d_time_stamp)
        cursor.execute(query)

        cursor.close()
        connection.commit()

        dot()

# Sample def
def publish():
	publisher.send_multipart(("heartbeat", "OHAI"))

def main():

    #worker5  = threading.Thread(target=ARTICLE_MASTER)
    #worker5.daemon  =True
    #worker5.start()
    #ioloop.PeriodicCallback(dot, period).start()

    ioloop.PeriodicCallback(MASTER_REALTIME, period).start()
    ioloop.IOLoop.instance().start()


if __name__ == "__main__":


    # run only once
    #ioloop.IOLoop.instance().run_sync(main)
    main()

#python -m tornado.autoreload IRSZeroMQServer.py