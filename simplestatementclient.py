#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
import logging
import time
import datetime

log = logging.getLogger()
log.setLevel('INFO')


class Config(object):
    #Set the Cassandra host
    cassandra_hosts = '127.0.0.1'

#These data generators are used to load data into the data model we've provided. This function returns an array of rows of data.
class Generator():
    def generate_data(self):
        results = []
        for i in range(90000,90100):
            for j in range(1,20):
                results.append([str(i),str(j),'coupon data', False, False, datetime.datetime.now()])
        return results

class SimpleClient(object):
#This class is used by all examples to set up generic functions like close, connect, and schema creation.    

    #Instantiate a session object to be used to connect to the database.
    session = None

    #Method to connect to the cluster and print connection info to the console
    def connect(self, nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
        self.session = cluster.connect()
        log.info('Connected to cluster: ' + metadata.cluster_name)
        for host in metadata.all_hosts():
            log.info('Datacenter: %s; Host: %s; Rack: %s',
                host.datacenter, host.address, host.rack)

    #Close the connection
    def close(self):
        self.session.cluster.shutdown()
        log.info('Connection closed.')

    #Create the schema. This will drop the existing schema when the application is run.
    def create_schema(self):
        self.session.execute("""DROP KEYSPACE IF EXISTS loyalty;""")
        self.session.execute("""CREATE KEYSPACE loyalty WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};""")
        self.session.execute("""
            CREATE TABLE loyalty.coupons (
                zip text,
                offer_id text,
                data text,
                liked boolean,
                clipped boolean,
                updated text,
                PRIMARY KEY((zip),offer_id)
            );
        """)

        log.info('Loyalty keyspace and schema created.')


class SimpleStatementClient(SimpleClient):
    #This class is an example of how to use simple statements. We simply execute with an argument of a string containing the statement.


    #Now we actually load the data. Here we use our prepared statement and bind values to it.
    def load_data(self):
        coupon_data = Generator().generate_data()
        #load generated data
        for row in coupon_data:
            self.session.execute("""INSERT INTO loyalty.coupons (zip, offer_id, data, liked, clipped, updated) VALUES (%s,%s,%s,%s,%s,%s);""" % (row[0],row[1],row[2],row[3],row[4],str(row[5])))
     

def main():
    logging.basicConfig()
    client = SimpleStatementClient()
    client.connect([Config.cassandra_hosts])
    client.create_schema()
    time.sleep(10)
    client.load_data()
    client.close()

if __name__ == "__main__":
    main()

