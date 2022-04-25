from scrapy.crawler import CrawlerProcess
from mapr_service import MAPR
from defiScrap.spiders.docsSpider import DocsspiderSpider
import pandas as pd 
import re
from evidence import Evidence
from database_service import DATABASE
from prepare_data import *
import requests
import yaml

mode = 'test'
excluded_sources =  [4,20,43,48,49]
config_file = '/home/joanna-sz/Documents/DeFi_Scrap/defiScrap/config.yml'

try:
  with open('/home/joanna-sz/Documents/DeFi_Scrap/defiScrap/config.yml') as file:
    paths = yaml.full_load(file)
except FileNotFoundError:
  print('No config.yaml in data folder.')

docs_frame, github_frame = sort_data(paths['paths'][mode]['datafile'])
owners = docs_frame['Domena'].to_list()
links = docs_frame['Link do dokumentacji'].to_list()

process = CrawlerProcess()

for i in range(0, len(links)):
    ps = re.search("\/[a-z]{1,}\.[a-zA-Z0-9[-]{1,}\.[a-zA-Z]{1,}",str(links[i]))
    if ps:
        allowed_domain = ps.group(0)
        allowed_domain = allowed_domain.replace("/", '')
    else:
        ps = re.search("\/[a-zA-Z0-9[-]{1,}\.[a-zA-Z]{1,}",str(links[i]))
        if ps :
            allowed_domain = ps.group(0)
            allowed_domain = allowed_domain.replace("/", '')
        else:
            allowed_domain = 'problem'
    filename = owners[i].replace('.', '_')
    filename = filename.replace('/', '_')
    process.crawl(DocsspiderSpider, start_urls = [str(links[i])], allowed_domains = [str(allowed_domain)], filename = filename)

process.start()   

for i in range(0, len(links)):
    filename = owners[i].replace('.', '_')
    filename = filename.replace('/', '_')
    columns = ["PAGE", "ADDRESS", "BLOCKCHAIN", "LINK"]
    if os.path.exists(paths['paths'][mode]['working_dir'] + f"/addresses_{filename}.csv"):
        current_data = pd.read_csv(paths['paths'][mode]['working_dir'] + f"/addresses_{filename}.csv", names= columns, header=None)
        addresses = current_data["ADDRESS"].to_list()
        addresses = [str(address).lower() for address in addresses]
        addresses = [address.replace('0x', '') for address in addresses]
        current_data["ADDRESS"] = pd.Series(addresses)
        current_data = current_data.drop_duplicates(subset="ADDRESS")
        current_data.to_csv(paths['paths'][mode]['working_dir'] + f"/addresses_{filename}.csv")
    else:
        print(f"couldn't find file {paths['paths'][mode]['working_dir']}/addresses_{filename}.csv")

evi = Evidence('test')

for i in range(0, len(links)):
    filename = owners[i].replace('.', '_')
    filename = filename.replace('/', '_')
    if os.path.exists(paths['paths'][mode]['working_dir'] + f"/addresses_{filename}.csv"):
        current_data = pd.read_csv(paths['paths'][mode]['working_dir'] + f"/addresses_{filename}.csv")
        pages = current_data["PAGE"].to_list()
        pages = set(pages)
        pages = list(pages)
        for i in range(0, len(pages)):
            evi.make_ev(pages[i])
    else:
        print(f"couldn't find file {paths['paths'][mode]['working_dir']}/addresses_{filename}.csv")


db = DATABASE(mode, config_file)
mapr = MAPR(mode, config_file)
uuid = paths['credentials']['det'][mode]['uuid']

for i in range(0, len(owners)):
  print(owners[i])
  ow_id = db.query_owner_id(str(owners[i]))
  print(ow_id)
  filename = owners[i].replace('.', '_')
  filename = filename.replace('/', '_')
  #Check if file with addresses exist
  if not os.path.exists(os.path.join(paths['paths'][mode]['working_dir'], f"addresses_{filename}.csv")):
    print(f"Can't find file for owner {filename}")
  else:
    current_data = pd.read_csv(os.path.join(paths['paths'][mode]['working_dir'], f"addresses_{filename}.csv"))
    addresses,blockchains,evi_links = filter_only_new(current_data, paths['paths'][mode]['all_addresses'])
    count_addresses = 0
    print(len(addresses))
    for i in range(0, len(addresses)):
        is_weird = check_if_weird(addresses[i])
        print(addresses[i] + " is weird?? " + is_weird)
        if is_weird:
            print(f"Address {addresses[i]} for owner {ow_id} looks weird, please check")
            requests.post(paths['credentials']['slack']['url'], json={"text": f"Address `{addresses[i]}` for owner {ow_id} from page {evi_links[i]} looks weird :face_with_raised_eyebrow:, please check"}, headers={"Content-Type": "application/json"})  
        else:
            current_ow = db.query_address(addresses[i])
            print(f"Current owners: {current_ow}")
            if type(current_ow) != list:
                current_ow = [current_ow]
            if ow_id not in current_ow:
              #check if given sources isn't already added and if address doesn't have other owners from specific sources
              if db.query_other_sources(addresses[i], excluded_sources):
                  #check if network is accepted
                  if blockchains[i] in ['ETH', 'BSC', 'ETC', 'RSK', 'FTM', 'AVAX', 'MATIC']:
                      print(f"In if current owners: {current_ow} for {addresses[i]}")
                      evi = os.path.join(paths['paths'][mode]['evidences'], evi_links[i].replace('/','_') + ".pdf")
                      if os.path.exists(evi):
                          evi_hash, ev_content = hash_file(evi,  evi_links[i],55, 9)
                          evi_in_db = db.query_evidence(evi_hash)
                          if evi_in_db == 0:
                              evi_added, evi_hash = mapr.add_to_mapr(evi, evi_links[i], 55, 9)
                              if evi_added == False:
                                print("Problem with adding evidence")
                              else:
                                evi_touple = tuple_bulder_ev_file(evi_hash, evi_links[i], evi, uuid, 55)
                                print(evi_touple)
                                db.insert_ev_files(evi_touple)
                                insert_tuple = tuple_bulder_addresses_owner(addresses[i], ow_id, uuid, 0, evi_hash, blockchains[i])
                                print(insert_tuple)
                                if db.insert_address_owner(insert_tuple):
                                    count_addresses += 1
                          else:
                            insert_tuple = tuple_bulder_addresses_owner(addresses[i], ow_id, uuid, 0, evi_hash, blockchains[i])
                            print(insert_tuple)
                            if db.insert_address_owner(insert_tuple):
                                count_addresses += 1
                      else:
                          print(f"Missing evidence {evi} for address {addresses[i]}")
    if count_addresses!= 0:
        requests.post(paths['credentials']['slack']['url'], json={"text": f"Added {count_addresses} addresses for owner {ow_id} in {mode} database"}, headers={"Content-Type": "application/json"})  
        break


