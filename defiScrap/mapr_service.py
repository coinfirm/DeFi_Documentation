from distutils.command.config import config
from mapr.ojai.storage.ConnectionFactory import ConnectionFactory
import sys
import yaml 
import subprocess
import os
import datetime
from logger import logger_setup
from prepare_data import *


class MAPR:

    def __init__(self, mode, config_file):
        ''' mode -> test/prod '''

        self.mode = mode
        self.load_conf_file(config_file)
        self.load_logger()
        self.connect()
        if mode == 'test':
            self.store_ = self.paths['credentials']['mapr']['test_store']
            self.is_store_exists(self.paths['credentials']['mapr']['test_store'])
        elif mode == 'prod':
            self.store_ = self.paths['credentials']['mapr']['prod_store']
            self.is_store_exists(self.paths['credentials']['mapr']['prod_store'])

    def load_logger(self):
        '''load logger'''

        self.logger = logger_setup('mapr-connection', self.paths['paths'][self.mode]['logs'])

    def load_conf_file(self, config_file):
        '''load configuration function - read configuraton, path, creds etc. from yaml file under data/ dir'''

        try:
            with open(config_file) as file:
                self.paths = yaml.full_load(file)
        except FileNotFoundError:
            print('No config.yaml in data folder.')


    def connect(self):
        ''' Create a connection to data access server'''

        connection_str = self.paths['credentials']['mapr']['host']+"?auth=basic&user="+self.paths['credentials']['mapr']['user']+"&password="+self.paths['credentials']['mapr']['passwd']+"&" \
                        "ssl=true&" \
                        "sslCA=/opt/mapr/conf/ssl_truststore.pem&"
                        # "sslTargetNameOverride=node1.mapr.com"
        try:
            self.connection = ConnectionFactory.get_connection(connection_str=connection_str)
            self.logger.info("Connection works")
        except Exception as e:
            self.logger.info(e)
            sys.exit(1)

    def is_store_exists(self, storePath):
        '''Get a store and assign it as a DocumentStore object '''

        if self.connection.is_store_exists(storePath):
            self.store = self.connection.get_store(storePath)
            self.logger.info("Created store.")
            return True
        return False

    def add_json_to_mapr_storage(self, _evidence):
        '''Add evidence to mapr'''

        evidence = _evidence
        # Create new document from json_document
        new_document = self.connection.new_document(dictionary=evidence)
        # Insert the OJAI Document into the DocumentStore
        try:
            self.store.insert_or_replace(new_document)
            self.logger.info("Succesfully added to MapR")
            return True
        except Exception as e:
            self.logger.error("\n{}".format(e))
            sys.exit(1)

    def close_connection(self):
        # close the OJAI connection
        self.connection.close()

    def check_clustering(self, addr_arr):
        '''Checking "combined clustering" for given addresses'''

        self.logger.info(f"Checking clustering for {addr_arr}")

        mapr_storage = self.connection.get_store(self.paths['tables']['address-cluster-data'])
        
        query = self.connection.new_query().build()

        query = {"$select": ["_id", "a"],
                 "$where": {"$in": {"_id": tuple(addr_arr)}}}


        options = {                    
            'ojai.mapr.query.result-as-document': True
            }

        query_result = mapr_storage.find(query, options=options)

        clustering_addr = []
        adr_tba = []
    
        query_res_arr = []

        for j in query_result:
            d = j.as_dictionary()
            query_res_arr.append(d['_id'])
        
        for i in addr_arr:
            if i in query_res_arr:
                clustering_addr.append(i)
            else:
                adr_tba.append(i)
        
        return adr_tba, clustering_addr



    def check_big_address(self, addr_arr):
        '''Checking if given addresses fals into "BIG ADDRESS" category'''

        self.logger.info(f"Checking if big addresses: {addr_arr}")

        options = {'ojai.mapr.query.result-as-document': True}
        
        mapr_storage = self.connection.get_store(self.paths['tables']['big-addresses'])
        
        query = {"$select": "address",
                 "$where": {"$in": {"address": addr_arr}}}
        
        query_result = mapr_storage.find(query, options)
        query_res_arr = []
        for j in query_result:
            d = j.as_dictionary()
            query_res_arr.append(d['address'])

        for i in addr_arr:
            if i in query_res_arr:
                return 0   # if address is in big-address array - verified = 0
            else:
                return 1     # adress tba is not a big-address - verified = 1

    def add_to_mapr(self, file_, domain, source, tag):

        ev_hash, file, file_size = insert_evidence_using_jar(file_, domain, source, tag)
        return self.insert_using_jar(ev_hash, file, file_size)

    def is_doc_uploaded(self, id, store):

        query = {"$select": ["_id"], "$where": {"$eq": {"_id": id}}}
        options = {"ojai.mapr.query.result-as-document": True}

        _store = self.connection.get_store(store)
        res = _store.find(query, options=options)
        try:  # should return False/empty if document doesn't exists
            return next(iter(res)).as_dictionary()
        except:
            return False

    def insert_using_jar(
        self,
        ev_hash: str,
        ev_name: str,
        size: str,
    ):

        self.logger.info("Before open")
        with open(os.path.join(self.paths["paths"][self.mode]["logs"], "output_of_jar.txt")) as fp:
            self.logger.info("Before subproces")
            subprocess.Popen(
                [
                    "java",
                    "-jar",
                    self.paths["paths"][self.mode]["mapr_jar"],
                    self.store_,
                    "True",
                    ev_hash,
                    ev_name.split("/")[-1],
                    str(size),
                    ev_name.split("/")[-1],
                ],
                stdout=fp,
            ).wait()
            self.logger.info("After subproces")
        uploaded = self.is_doc_uploaded(id=ev_hash, store=self.store_)
        if uploaded:

            self.logger.info("Uploaded to mapr")
            self.logger.info(
                "Removing file: {} from merged".format(ev_name.split("/")[-1])
            )
            rm_path = os.path.join("merged",ev_name.split("/")[-1])
            os.system(f"rm {rm_path}")
            return True, ev_hash
        else:
            self.logger.error("Failed uploading using .jar")
            return False, '-1'