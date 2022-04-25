import os
import base64
import hashlib
import datetime
import pandas as pd
import shutil

'''module that prepares data for inserting to propriate tables in database'''


def mapr_ev(file):

    file_size = get_size(file)
    ev_hash, ev_bytes = hash_file(file)
    file_name = os.path.basename(file)
    fixed_dict = {'size':file_size, 'content': ev_bytes, '_id': ev_hash, 'hash': ev_hash, 'name': file_name}
    return fixed_dict

def hash_file(file, url, source, tag):

    evidence = open(file, "rb").read()
    content = base64.b64encode(evidence)  # content

    to_hash = f"{source}-{tag}-{url}-{content}".encode("utf-8")
    hash_obj = hashlib.sha1(to_hash)
    ev_hash = str(hash_obj.hexdigest())
    return ev_hash, content

def insert_evidence_using_jar(file, domain, source, tag):

    dest = os.path.join("/home/joanna-sz/merged/", file.split('/')[-1])
    shutil.copyfile(file, dest)
    file_size = get_size(dest)
    ev_hash, ev_bytes = hash_file(dest, domain, source, tag)

    return ev_hash, dest, file_size

def get_time():

    now = datetime.datetime.now()
    timestamp = datetime.datetime.timestamp(now)
    timestamp -= 3600
    time = datetime.datetime.fromtimestamp(timestamp)
    return (time.strftime("%Y-%m-%d %H:%M:%S"))

def get_name(file):

    return os.path.basename(file.split('/')[-1]) # file_name

def get_size(file):

    return os.stat(file).st_size # file_size


def tuple_bulder_ev_file(ev_hash,url, file, uuid, source):
    file_name = get_name(file)
    if len(file_name) > 80:
        file_name = file_name[:80]
    file_size = get_size(file)
    date_time = get_time()

    address_dict = {
        "ev_file_hash": ev_hash,
        "name": file_name,
        "url": url,
        "size": file_size,
        "sql_content_size": file_size,
        "mapr_content_size": file_size,
        "storage_type": 'MAPR',
        "source_id": source, 
        "tag_id": '1',
        "added": date_time,
        "added_by": uuid
    }

    insert_tuple = [address_dict[k] for k in address_dict.keys()]
    insert_tuple = tuple(insert_tuple)
    return [insert_tuple]

def tuple_bulder_addresses_flag(addr, flag_id,  file, uuid, verified, network):
    ev_hash, content = hash_file(file)
    date_time = get_time()

    if verified == 1:
        active = 1
        verified_by = uuid
        verified = date_time

    else:
        active = None
        verified_by = None
        verified = None

    address_dict = {
        "address": addr,
        "flag_id": flag_id,
        "ev_file_hash": ev_hash,
        "blockchain": network, #BSC/ETH
        "block_height_from": None,
        "block_height_to": None,
        "active": active,
        "added": date_time,
        "added_by": uuid,
        "verified": verified,
        "verified_by": verified_by
    }
    insert_tuple = [address_dict[k] for k in address_dict.keys()]
    insert_tuple = tuple(insert_tuple)
    return [insert_tuple]


def tuple_bulder_addresses_description(addr, uuid, verified, network):
    date_time = get_time()

    if verified == 1:
        active = 1
        verified_by = uuid
        verified = date_time

    else:
        active = None
        verified_by = None
        verified = None

    address_dict = {
        "address": addr,
        "description": 'Address related to a scam.',
        "blockchain": network, #BSC/ETH
        "active": active,
        "added": date_time,
        "added_by": uuid,
        "verified": verified,
        "verified_by": verified_by
    }
    insert_tuple = [address_dict[k] for k in address_dict.keys()]
    insert_tuple = tuple(insert_tuple)
    return [insert_tuple]


def tuple_bulder_addresses_owner(addr, owner, uuid, verified, ev_hash, network):
    date_time = get_time()

    if verified == 1:
        active = 1
        verified_by = uuid
        verified = date_time

    else:
        active = None
        verified_by = None
        verified = None

    address_dict = {
        "address": addr,
        "owner_id": owner,
        "ev_file_hash": ev_hash,
        "blockchain": network,
        "active": active,
        "added": date_time,
        "added_by": uuid,
        "verified": verified,
        "verified_by": verified_by
    }
    insert_tuple = [address_dict[k] for k in address_dict.keys()]
    insert_tuple = tuple(insert_tuple)
    return [insert_tuple]

def sort_data(file):
    data = pd.read_excel(file)
    if_address = data['Czy są adresy?'].to_list()
    if_address = [str(ad).lower() for ad in if_address]
    data['Czy są adresy?'] = pd.Series(if_address)
    data = data.loc[data['Czy są adresy?'] == 'tak']
    links = data['Link do dokumentacji'].to_list()
    github_data = [link for link in links if link.find('https://github.com')!=-1]
    github_frame = data.loc[data['Link do dokumentacji'].isin(github_data)]
    docs_frame = data.loc[~data['Link do dokumentacji'].isin(github_data)]
    docs_frame.to_csv("/home/joanna-sz/Documents/DeFi_Scrap/data/docs.csv")
    github_frame.to_csv("/home/joanna-sz/Documents/DeFi_Scrap/data/git.csv")
    return docs_frame, github_frame


        
def check_if_weird(address):
    l = len(address)
    if l < 38:
        return True

    charMap = {}
    weird = False
    for char in address:
        if char not in charMap.keys():    
            charMap[char] = 1       
        else:
            charMap[char] += 1

    for char in charMap:
        if charMap[char] > 20:
            weird = True


    count = 0
 
    # Find the maximum repeating
    # character starting from str[i]
    for i in range(l):
         
        cur_count = 1
        for j in range(i + 1, l):
     
            if (address[i] != address[j]):
                break
            cur_count += 1
 
        # Update result if required
        if cur_count > count :
            count = cur_count


    if count > 5 or weird == True:
        return True
    else:
        return False
        
def filter_only_new(new_data, main_file):
    main_data = pd.read_csv(main_file)
    new_addresses = new_data['ADDRESS'].to_list()
    all_addresses = main_data['ADDRESS'].to_list()
    for a in new_addresses:
        if a in all_addresses:
            new_data = new_data.drop(new_data[new_data['ADDRESS']==a].index)

    addresses = new_data['ADDRESS'].to_list()
    blockchains = new_data['BLOCKCHAIN'].to_list()
    evi_links = new_data['Link'].to_list()

    return addresses, blockchains, evi_links

