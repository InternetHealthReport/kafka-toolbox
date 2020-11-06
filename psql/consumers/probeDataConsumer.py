"""
Reads latest probe data from Kafka
"""

from kafka import KafkaConsumer
# from utils import haversine
import msgpack

class ProbeDataConsumer():
    def __init__(self,asnFilters=[],countryFilters=[],proximityFilters=[],startTS=None,endTS=None):
        self.topicName = "ihr_atlas_probe_archive"

        self.consumer = KafkaConsumer(self.topicName,auto_offset_reset="earliest",
                bootstrap_servers=['kafka1:9092','kafka2:9092'],
                consumer_timeout_ms=1000,
                value_deserializer=lambda v: msgpack.unpackb(v, raw=False))

        self.asnFilters = asnFilters
        self.countryFilters = countryFilters
        self.proximityFilters = proximityFilters

        self.proximityThreshold = 50

        self.observers = []

        self.startTS = startTS
        self.endTS = endTS

    def attach(self,observer):
        if observer not in self.observers:
            self.observers.append(observer)

    def notifyObservers(self,data):
        for observer in self.observers:
            observer.probeDataProcessor(data)

    def start(self):
        for message in self.consumer:
            record = message.value

            self.notifyObservers(record)


"""
#EXAMPLE

probeCon = ProbeDataConsumer(asnFilters=[57169],countryFilters=["AT"],proximityFilters=[[16.4375,47.3695],[15.4375,47.3695]])
probeCon.attach(object) #Attach object that defines probeDataProcessor function
probeCon.start()"""
