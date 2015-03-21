import socket
import json
import os
from ConfigParser import SafeConfigParser

class ReservoirClient:
    def __init__(self):
        self.initialize_configs()
        self.create_socket()
        self.connect()

    def initialize_configs(self):
        config = SafeConfigParser()
        config.read([
            os.path.join(os.path.dirname(__file__), 'default.conf'),
            # any other files to overwrite defaults here
        ])

        self.host=config.get('client', 'server_host')
        self.port=config.getint('client', 'server_port')
        self.protocol=config.get('client', 'protocol')
        
        # set the protocol to follow
        if self.protocol not in ['TCP', 'UDP']:
            self.protocol = 'TCP' 

    def create_socket(self):
        if self.protocol == 'TCP':
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        elif self.protocol == 'UDP':
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        else:
            self.socket = socket.socket()
        
    def connect(self):
        if self.protocol == 'TCP':
            # print 'connecting to the server %s' % (self.host)
            self.socket.connect((self.host, self.port))

        if self.protocol == 'UDP':
            # print 'No connection required for UDP'
            pass

    def set(self, key, value, expiry):
        batch = [{
            'key': key,
            'data': value,
            'expiry': str(expiry)
        }]
        data_string = json.dumps(batch)
        data = "SET %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json[0].get("data", None)

    def set_batch(self, items):
        batch = []
        for item in items:
            if not item.get("key", None):
                continue
            element = {
                "key": item.get("key"),
                "data": item.get("value", None),
                "expiry": item.get("expiry", None),
            }
            batch.append(element)

        data_string = json.dumps(batch)
        data = "SET %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json

    def get(self, key):
        batch = [{
            'key': key
        }]
        data_string = json.dumps(batch)
        data = "GET %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json[0].get("data", None)

    def get_batch(self, keys):
        batch = []
        for key in keys:
            if not key:
                continue
            element = {'key': key}
            batch.append(element)

        data_string = json.dumps(batch)
        data = "GET %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json

    def get_bucket(self, bucket):
        batch = [{
            'bucket': bucket
        }]
        data_string = json.dumps(batch)
        data = "BKT %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json

    def delete(self, key):
        batch = [{
            'key': key
        }]
        data_string = json.dumps(batch)
        data = "DEL %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json

    def delete_batch(self, keys):
        batch = []
        for key in keys:
            if not key:
                continue
            element = {'key': key}
            batch.append(element)

        data_string = json.dumps(batch)
        data = "DEL %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json

    def set_dependant(self, key, value, expiry, parent_key):
        batch = [{
            'key': key,
            'data': value,
            'expiry': str(expiry),
            'parent_key': parent_key
        }]
        data_string = json.dumps(batch)
        data = "DEP %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json[0].get("data", None)

    def set_dependant_batch(self, items):
        batch = []
        for item in items:
            if not item.get("key", None):
                continue
            element = {
                "key": item.get("key"),
                "data": item.get("value", None),
                "expiry": item.get("expiry", None),
                'parent_key': item.get("parent_key")
            }
            batch.append(element)

        data_string = json.dumps(batch)
        data = "DEP %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json

    def icr(self, key, expiry=0):
        # send expiry=0 for already existing key for ICR
        # need to imporve the evaluation for ICR on the server side
        batch = [{
            'key': key,
            'expiry': str(expiry)
        }]
        data_string = json.dumps(batch)
        data = "ICR %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json

    def icr_batch(self, keys):
        batch = []
        for key in keys:
            if not key:
                continue
            element = {'key': key}
            batch.append(element)

        data_string = json.dumps(batch)
        data = "ICR %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json

    def dcr(self, key, expiry=0):
        # send expiry=0 for already existing key for DCR
        batch = [{
            'key': key,
            'expiry': str(expiry)
        }]
        data_string = json.dumps(batch)
        data = "DCR %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json[0].get("data", None)

    def dcr_batch(self, keys):
        batch = []
        for key in keys:
            if not key:
                continue
            element = {'key': key}
            batch.append(element)

        data_string = json.dumps(batch)
        data = "DCR %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json

    def tmr(self, key):
        batch = [{
            'key': key,
        }]
        data_string = json.dumps(batch)
        data = "TMR %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json[0].get("data", None)


    def ota(self, key, value, expiry):
        batch = [{
            'key': key,
            'data': value,
            'expiry': str(expiry)
        }]
        data_string = json.dumps(batch)
        data = "OTA %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json[0].get("data", None)

    def ota_batch(self, items):
        batch = []
        for item in items:
            if not item.get("key", None):
                continue
            element = {
                "key": item.get("key"),
                "data": item.get("value", None),
                "expiry": item.get("expiry", None),
            }
            batch.append(element)

        data_string = json.dumps(batch)
        data = "OTA %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json

    def tpl(self, key, value, expiry):
        batch = [{
            'key': key,
            'data': value,
            'expiry': str(expiry)
        }]
        data_string = json.dumps(batch)
        data = "TPL %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json[0].get("data", None)

    def tpl_batch(self, items):
        batch = []
        for item in items:
            if not item.get("key", None):
                continue
            element = {
                "key": item.get("key"),
                "data": item.get("value", None),
                "expiry": item.get("expiry", None),
            }
            batch.append(element)

        data_string = json.dumps(batch)
        data = "TPL %s" % (data_string,)
        result = self.send(data)
        result_json = json.loads(result)
        return result_json

    def get_or_set(self, key, value, expiry):
        batch = [{
            'key': key,
            'data': value,
            'expiry': str(expiry)
        }]
        data_string = json.dumps(batch)
        data = "GOS %s" % (data_string,)
        return self.send(data)

    def ping_server(self):
        data = "PING"
        return True if self.send(data) == 1 else False # this should return boolean only

    # get the entire tree <key> as starting parent
    def get_with_dependants(self, key):
        pass

    def send(self, data, expect_return=True):
        if self.protocol == 'TCP':
            self.socket.send(data)
            if expect_return:
                response = self.socket.recv(1024)
                if response:
                    response_json = json.loads(response)
                    return response_json.get('data', None)
                else:
                    return {}
            else:
                return True

        if self.protocol == 'UDP':
            self.socket.sendto(data, (self.host, self.port))
            if expect_return:
                packet = self.socket.recvfrom(1024)
                response = packet[0]
                address = packet[1]
                return response
