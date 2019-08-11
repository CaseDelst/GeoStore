"""
Very simple HTTP server in python for logging requests
Usage::
    ./server.py [<port>]
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import json
import pandas as pd

class S(BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        logging.info("GET request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        self._set_response()
        self.wfile.write("GET request for {}".format(self.path).encode('utf-8'))

    
    def do_POST(self):
        content_length = int(self.headers['Content-Length']) # <--- Gets the size of data
        post_data = self.rfile.read(content_length) # <--- Gets the data itself
        #logging.info("POST request,\nPath: %s\nHeaders:\n%s\n\nBody:\n%s\n",
         #      str(self.path), str(self.headers), post_data.decode('utf-8'))

        body = json.loads(post_data.decode('utf-8'))
        locations = body['locations']
        frame = pd.DataFrame(columns=['timestamp', 'coordinates', 'altitude', 'data_type', 'speed', 'motion', 'battery_level', 'battery_state', 'accuracy', 'wifi'])
        i = 0
        for entry in locations:
            data_type = entry['geometry'].get('type')
            coordinates = str(entry['geometry']['coordinates'][0]) + ',' + str(entry['geometry']['coordinates'][1])
            try: 
                motion = ','.join(entry['properties'].get('motion'))
            except (IndexError, TypeError) as e:
                motion = 'None'
            speed = entry['properties'].get('speed')
            battery_level = entry['properties'].get('battery_level')
            altitude = entry['properties'].get('altitude')
            battery_state = entry['properties'].get('battery_state')
            accuracy = str(entry['properties'].get('horizontal_accuracy')) + ',' + str(entry['properties'].get('vertical_accuracy'))
            timestamp = entry['properties'].get('timestamp')
            wifi = entry['properties'].get('wifi')
            temp = [timestamp, coordinates, altitude, data_type, speed, motion, battery_level, battery_state, accuracy, wifi]
            frame.loc[i] = temp
            i += 1

        print(frame)
        frame.to_csv('data.csv', index=False)
        self._set_response()
        self.wfile.write("POST request for {}".format(self.path).encode('utf-8'))

def run(server_class=HTTPServer, handler_class=S, port=8080):
    logging.basicConfig(level=logging.INFO)
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    logging.info('Starting Endpoint For Overland...\n')
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
    
        
