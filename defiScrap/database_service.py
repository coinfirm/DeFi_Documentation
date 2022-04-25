import mysql.connector, sys, yaml, os, re
from logger import logger_setup
from fuzzywuzzy import fuzz

class DATABASE:
    '''Class for database connection'''

    def __init__(self, mode, config_file):   # mode => test/prod
        
        self.mode = mode
        self.working_dir = os.getcwd().rsplit('/',1)[0]
        self.load_conf_file(config_file)
        self.load_logger()
        self.db_connect()

    def load_logger(self):
        '''load logger'''

        self.logger = logger_setup('database', self.paths['paths'][self.mode]['logs'])


    def load_conf_file(self, config_file):
        '''load configuration function - read configuraton, path, creds etc. from yaml file under data/ dir'''

        try:
            with open(config_file) as file:
                self.paths = yaml.full_load(file)
        except FileNotFoundError:
            print('No config.yaml in data folder.')

    def db_connect(self):
        '''connect to SQL database'''

        database_info = self.paths['credentials']['det'][self.mode]
        
        try:
            database_connection = mysql.connector.connect(
              host=database_info['host'],
              user=database_info['user'],
              passwd=database_info['passwd'],
              port=database_info['port'],
              database='det',
              autocommit=True,
            )
            self.database_connection = database_connection
        except mysql.connector.Error as error:
            self.logger.info("Failed to connect {}".format(error))
            sys.exit(1)



    def query_owner_id(self,page):

        print(page)
        ps = re.search("\/[a-z]{1,}\.[a-zA-Z0-9[-]{1,}\.[a-zA-Z]{1,}",str(page))
        if ps:
                p = ps.group(0)
                p = p.replace("/", '')
        else:
            ps = re.search("\/[a-zA-Z0-9[-]{1,}\.[a-zA-Z]{1,}",str(page))
            if ps :
                    p = ps.group(0)
                    p = p.replace("/", '')
            else:
                    return "PROBLEM"
                    
        sql_query = '''
                SELECT id, domain
                FROM det.owners 
                WHERE domain 
                LIKE '%{}%' '''.format(p)

        if (self.database_connection.is_connected()):
            cursor = self.database_connection.cursor(prepared=True)
            cursor.execute(sql_query)
            row_ = cursor.fetchall()

            if len(row_) >= 1:
                scores = []
                for i in range(0,len(row_)):
                    p_found = row_[i][0]                    
                    ps_found = re.search("\/[a-z]{1,}\.[a-zA-Z0-9[-]{1,}\.[a-zA-Z]{1,}",str(row_[i][1]))
                    if ps_found:
                        p_found = ps_found.group(0)
                        p_found = p_found.replace("/", '')
                    else:
                        ps_found = re.search("\/[a-zA-Z0-9[-]{1,}\.[a-zA-Z]{1,}",str(row_[i][1]))
                        if ps_found:
                            p_found = ps_found.group(0)
                            p_found = p_found.replace("/", '')
                    scores.append([row_[i][0],fuzz.ratio(p,p_found)])
                scores = [score[0] for score in scores if score[1] == 100]
                if len(scores) == 1:
                    return scores[0]
                elif len(scores) > 1:
                    return -2
                else:
                    return -1
                

    def query_address(self,address):

        sql_query = '''
                    select owner_id,active from det.address_owners where address = '{}';'''.format(address)

        if (self.database_connection.is_connected()):
            cursor = self.database_connection.cursor(prepared=True)
            cursor.execute(sql_query)
            row_ = cursor.fetchall()

            if not row_:
                return -1
            elif len(row_) == 1:
                if row_[0][1] == 0:
                    return -1
                else:
                    return row_[0][0]
            else :
                ow_set = set(row_)
                ow_set = list(ow_set)
                if len(ow_set) == 1:
                        return ow_set[0][0]
                else:
                    found = [item[0] for item in row_]
                    if len(found) == 1:
                        return found[0]
                    else:
                        return found
    
    def query_other_sources(self,address, sources):

        sql_query = '''
            select ev.source_id from det.address_owners as ao join det.ev_files as ev on ao.ev_file_hash = ev.ev_file_hash
            where  address = '{}';'''.format(address)

        safe = True

        if (self.database_connection.is_connected()):
            cursor = self.database_connection.cursor(prepared=True)
            cursor.execute(sql_query)
            row_ = cursor.fetchall()

            for i in range(0, len(row_)):
                if row_[i][0] in sources:
                    safe = False
                    
        return safe
     

    def query_evidence(self,hash):

            sql_query = '''
                       select ev_file_hash from det.ev_files where ev_file_hash = '{}';'''.format(hash)

            if (self.database_connection.is_connected()):
                cursor = self.database_connection.cursor(prepared=True)
                cursor.execute(sql_query)
                row_ = cursor.fetchall()

                if not row_:
                    return 0
                else:
                    return 1

    def insert_ev_files(self, insert_tuple):

        try:
            if (self.database_connection.is_connected()):
                cursor = self.database_connection.cursor()
                sql_insert_query = """ INSERT INTO ev_files
                            (ev_file_hash, name, url, size, sql_content_size, mapr_content_size, storage_type, source_id, tag_id, added, added_by) VALUES (%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s)"""
                cursor.execute(sql_insert_query, insert_tuple[0])
                # self.database_connection.commit()
                self.logger.info("Data inserted successfully into ev_files table using the prepared statement")
                return True
        
        except Exception as e:
            self.logger.error("\n{}".format(e))
            return False

    def insert_address_flag_manual(self, insert_tuple):

        try:
            if (self.database_connection.is_connected()):
                cursor = self.database_connection.cursor()
                sql_insert_query = """ INSERT INTO address_flags_manual 
                (address, flag_id, ev_file_hash, blockchain, block_height_from, block_height_to, active, added, added_by, verified, verified_by)
	            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(sql_insert_query, insert_tuple[0])
                # self.database_connection.commit()
                self.logger.info("Data inserted successfully into address_flag_manual table using the prepared statement")
                return True

        except Exception as e:
            self.logger.error("\n{}".format(e))
            return False
            

    def insert_address_decription(self, insert_tuple):

        try:
            if (self.database_connection.is_connected()):
                cursor = self.database_connection.cursor()
                sql_insert_query = """ INSERT INTO address_description
                            (address, description, blockchain, active, added, added_by, verified, verified_by) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(sql_insert_query, insert_tuple[0])
                # self.database_connection.commit()
                self.logger.info("Data inserted successfully into address_flag_description table using the prepared statement")
                return True

        except Exception as e:
            self.logger.error("\n{}".format(e))
            return False

            
    def insert_address_owner(self, insert_tuple):

        try:
            if (self.database_connection.is_connected()):
                cursor = self.database_connection.cursor()
                sql_insert_query = """ INSERT INTO address_owners
                            (address, owner_id,ev_file_hash, blockchain, active, added, added_by, verified, verified_by) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(sql_insert_query, insert_tuple[0])
                # self.database_connection.commit()
                self.logger.info("Data inserted successfully into address_owners table using the prepared statement")
                return True
                
        except Exception as e:
            self.logger.error("\n{}".format(e))
            return False
