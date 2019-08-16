
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import json
import sys
import socket
import os
import pandas as pd
import math
import mysql.connector as connector
import traceback

class Server(BaseHTTPRequestHandler):

    #Sets the header response
    def _set_response(self):
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json',)
        self.end_headers()
        
    #Reacts to the POST from the app
    def do_POST(self):

        #Gets the size of data
        content_length = int(self.headers['Content-Length']) 
        
        #Retrieves the data itself
        post_data = self.rfile.read(content_length)
        
        #logging.info("POST request,\nPath: %s\nHeaders:\n%s\n\nBody:\n%s\n",
        #      str(self.path), str(self.headers), post_data.decode('utf-8'))

        #Gets the actual body data from the POST, decodes
        body = json.loads(post_data.decode('utf-8'))

        #Calls function created to sort the JSON data into a pandas dataframe
        df = self.parseJSON(body)

        #Stores the data in my custom SQL database 
        responseBool = self.storeInSQL(df)

        #Responds to the POST request
        self._set_response()

        #Writes the output of the POST request to the phone
        if responseBool: json_string = json.dumps({"result":"ok"}) #If the data was stored correctly
        else: json_string = json.dumps({"result":"error"}) #If there was an error, don't delete the data
        
        #Sends the response to the app
        self.wfile.write(json_string.encode('utf-8'))

    def parseJSON(self, jsonText):
        
        #Selects the location list
        locations = jsonText['locations']

        #Instantiates a pandas dataframe
        frame = pd.DataFrame(columns=['timestamp', 'coordinates', 'altitude', 'data_type', 'speed', 'motion', 'battery_level', 'battery_state', 'accuracy', 'wifi'])
        
        #Row counter
        i = 0
        
        #Loop through values of array inside locations (dictionaries)
        for entry in locations:

            #Store all relevant pieces of information that I want
            data_type = entry['geometry'].get('type')
            coordinates = str(entry['geometry']['coordinates'][0]) + ',' + str(entry['geometry']['coordinates'][1])
            
            try: #Motion doesn't always have properties in it
                motion = ','.join(entry['properties'].get('motion'))
            except (IndexError, TypeError) as e: 
                motion = 'None' #if there are no properties, assign None
            
            speed = entry['properties'].get('speed')
            if speed is None: speed = -1
            
            battery_level = entry['properties'].get('battery_level')
            if battery_level is None: battery_level = -1
            
            altitude = entry['properties'].get('altitude')
            if altitude is None: altitude = -1000
            
            battery_state = entry['properties'].get('battery_state')
            if battery_state is None: battery_state = ''
            
            accuracy = str(entry['properties'].get('horizontal_accuracy')) + ',' + str(entry['properties'].get('vertical_accuracy'))
            if accuracy is None: accuracy = '0,0'
            
            timestamp = entry['properties'].get('timestamp')
            
            wifi = entry['properties'].get('wifi')
            if wifi is None: wifi = ''
            
            #Creates an array of all the important values in the correct order
            temp = [timestamp, coordinates, altitude, data_type, speed, motion, battery_level, battery_state, accuracy, wifi]
            
            #Sets the row of the dataframe to the values in temp, correctly ordered
            frame.loc[i] = temp
            
            #Increment row counter
            i += 1
        
        #Returns the complete dataframe
        frame.fillna(0)
        return frame

    def storeInSQL(self, df):
        
        #Connects the database to my script
        with open('keys.txt', 'r') as file:

            #Replaces the newlines in the file with spaces
            keysLine = file.read().replace('\n', ' ')
            
            #Split the keys into a list by delimiting space
            keysList = keysLine.split(' ')
        
        print("\nConnecting to database... \nSchema: " + keysList[3] + '\nUser:   ' + keysList[1])
        
        #Uses the connection information in keys.txt to create a connection to database
        mydb = connector.connect(host=keysList[0],
                                 user=keysList[1],
                                 passwd=keysList[2],
                                 database=keysList[3])

        #For testing
        #return False

        #The navigator of the database
        mycursor = mydb.cursor()

        #Gets the row 
        countRow, countColumn = df.shape

        #An array for each row, and the overall dataframe
        tempArr = []
        totalArr = []
        
        #Iterate over every row in the table
        for item in df.itertuples():

            #Range of the data that I want from the dataframe columns
            for i  in range(1, 11):
                if item[i] == 'nan' or item[i] == 'None' or item[i] == 'None,None': tempArr.append(None)
                else: tempArr.append(item[i])
                
            #Format the array into a tuple, and add it to overall table
            tempArr = tuple(tempArr)
            print(tempArr)
            totalArr.append(tempArr)
            tempArr = []
        
        
        try:
            sqlFormula = """INSERT IGNORE INTO location
                            (timestamp, coordinates, altitude, data_type, speed, motion, battery_level, battery_state, accuracy, wifi) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            mycursor.executemany(sqlFormula, list(totalArr))
            mydb.commit()
            print('\nSuccess! Stored ' + str(countRow) + ' values in SQL database\n')
            return True
        except:
            traceback.print_exc()
            print("ERROR: Not sending confirmation. Please check database")
            return False 
           

def run(server_class=HTTPServer, handler_class=Server, port=8080):
    logging.basicConfig(level=logging.INFO)
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    print(s.getsockname()[0])
    
    logging.info('Starting Endpoint For Overland on IP: ' + str(s.getsockname()[0]) + ':' + str(port) + '\n') 
    s.close()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info('Killing server...\n')

if __name__ == '__main__':
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
    