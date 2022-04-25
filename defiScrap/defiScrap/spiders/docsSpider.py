import scrapy
import re
import csv
from fp.fp import FreeProxy


class DocsspiderSpider(scrapy.Spider):
    name = 'docsSpider'
    custom_settings = { 'DOWNLOD_DELAY': 2 }        
    proxy = FreeProxy(rand=True, timeout=1).get()

    def parse(self, response):
        print(response.url)
        matches = re.findall(r'0x[0-9a-fA-F]{40}\b', response.text)
        next_page = response.css('a::attr(href)').getall()

        whole_next_page = [page for page in next_page if page.find('@') == -1 and page.find('governance') == -1 and page.find('voting') == -1 and page.find('guide') == -1]
        whole_next_page = set(whole_next_page)
        whole_next_page = list(whole_next_page)
        kovan_eth_address = [[page, "KOVAN ETH"]  for page in whole_next_page if (page.find('kovan')!=-1 and page.find('etherscan.io')!=-1)]
        eth_address = [[page, "ETH"] for page in whole_next_page if (page.find('etherscan.io')!=-1 and page not in kovan_eth_address)]
        bsc_address = [[page, "BSC"]  for page in whole_next_page if page.find('bsc')!=-1]
        polygon_address = [[page, "MATIC"]  for page in whole_next_page if page.find('polygon')!=-1]
        avalanche_address = [[page, "AVALANCHE"]  for page in whole_next_page if page.find('ava')!=-1]
        arbiscan_address = [[page, "ARBI"]  for page in whole_next_page if page.find('arbiscan')!=-1]
        fantom_address = [[page, "FANTOM"] for page in whole_next_page if (page.find('ftmscan')!=-1 or page.find('fantom')!=-1)]
        kovan_optimisitic_address = [[page, "KOVAN OPTIMISTIC"]  for page in whole_next_page if (page.find('kovan')!=-1 and page.find('optimism')!=-1)]
        optimisitic_address = [[page, "OPTIMISTIC"]  for page in whole_next_page if (page.find('optimism')!=-1 and page not in kovan_optimisitic_address)]
        identified_pages = eth_address + kovan_eth_address + bsc_address + polygon_address + avalanche_address + arbiscan_address + fantom_address + optimisitic_address +kovan_optimisitic_address
        identified_addresses = []
        identified_blockchains = []
        for p in identified_pages:
            match = re.findall(r'0x[0-9a-fA-F]{40}\\',p[0])
            if match:
                identified_addresses.append(match[0])
                identified_blockchains.append(p[1])
        unidentified_addresses = [a for a in matches if a not in identified_addresses]

        with open(f"/home/joanna-sz/Documents/DeFi_Scrap/data/addresses_{self.filename}.csv", mode = 'a', newline = '', encoding = 'utf-8') as f:
            writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            for i in range(0,len(identified_addresses)):
                writer.writerow([response.url,identified_addresses[i], identified_blockchains[i],identified_pages[i][0]])
            for a in unidentified_addresses:
                writer.writerow([response.url,a, "ETH", "None"])   
            
        for page in whole_next_page:
            next_url = response.urljoin(page)
            yield scrapy.Request(next_url, callback=self.parse)
            # , meta={"proxy": self.proxy})
