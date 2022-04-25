from importlib.resources import path
import os
import time, datetime
import random
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import json
import glob
from pyvirtualdisplay import Display
from PyPDF2 import PdfFileMerger
import yaml
from logger import logger_setup
from fake_useragent import UserAgent
import textwrap
import Xlib
import fpdf

class Evidence:
    ''' create nice eivdence for given url,
    '''
    def __init__(self, mode, config_file):

        self.mode = mode
        self.load_conf_file(config_file)
        self.load_logger()

        # start virtual display for printing webiste to pdf
        self.display = Display(visible=0, size=(1024, 768),use_xauth=True)
        self.display.start()
        
        import pyautogui
        self.pyautogui = pyautogui
        self.pyautogui._pyautogui_x11._display = Xlib.display.Display(os.environ['DISPLAY'])

        self.CHROME_PATH = '/usr/bin/google-chrome'
        self.CHROMEDRIVER_PATH = '/usr/bin/chromedriver'
        self.chrome_options = Options()
        self.chrome_options.binary_location = self.CHROME_PATH

        # settings for saving pdf ??????
        settings = {
                "recentDestinations": [{
                        "id": "Save as PDF",
                        "origin": "local",
                        "account": "",
                        "default_directory": self.paths['paths'][self.mode]['merge'],
                    }],
                    "selectedDestinationId": "Save as PDF",
                    "scalingType": 3,
                    "scaling": "40",
                    "version": 2
                    }

       # additional preferences for saving document in given path             
        prefs = {'printing.print_preview_sticky_settings.appState': json.dumps(settings),
                 'savefile.default_directory': self.paths['paths'][self.mode]['merge'],
                 "translate":{"enabled":"True"}}

        self.chrome_options.add_experimental_option('prefs', prefs)
        self.chrome_options.add_argument('--kiosk-printing')

        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--lang=en-GB')

        self.chrome_options.add_extension(self.paths['paths'][self.mode]['extensions']+'/printfriendly.crx')
        self.chrome_options.add_extension(self.paths['paths'][self.mode]['extensions']+'/gg_translate.crx')

        self.driver = webdriver.Chrome(
            executable_path=self.CHROMEDRIVER_PATH,
            options=self.chrome_options
            )
            

    def restart(self):
        # start virtual display for printing webiste to pdf

        self.display = Display(visible=0, size=(1024, 768),use_xauth=True)
        self.display.start()
        
        self.pyautogui._pyautogui_x11._display = Xlib.display.Display(os.environ['DISPLAY'])        

        self.driver = webdriver.Chrome(
            executable_path=self.CHROMEDRIVER_PATH,
            options=self.chrome_options
            )

    def load_logger(self):
        '''Load logger'''

        self.logger = logger_setup('evidence', self.paths['paths'][self.mode]['logs'])


    def load_conf_file(self, config_file):
        '''Load configuration function - read configuraton, path, creds etc. from yaml file under data/ dir'''

        try:
            with open(config_file) as file:
                self.paths = yaml.full_load(file)
        except FileNotFoundError:
            self.logger.info('No config.yaml in data folder.')
    
    def random_wait(self):

        wait_time = random.randint(5, 10)
        self.logger.info(f"Waiting for {wait_time} seconds")
        time.sleep(wait_time)
    
    def empty_folder(self,folder):

        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))
    
    def make_ev(self,url):

        number = 0
        status = 'bad'
        if os.path.exists(self.paths['paths'][self.mode]['evidences'] + '/{}.pdf'.format(url.replace('/','_'))):
            print(f'Evidence for {url} exists - skipping')
            return True
        while status == 'bad' and number < 3:
            try:
                self.empty_folder(self.paths['paths'][self.mode]['merge'])
                number = self.make_basic_ev(url,number)
                time.sleep(5)
                self.logger.info("After basic evidence")
                self.make_pfp_ev()
                time.sleep(15)
                self.merge_ev(self.paths['paths'][self.mode]['merge'],url.replace('/','_')+"ev")
                time.sleep(10)
                if os.path.exists(self.paths['paths'][self.mode]['evidences'] + '/{}.pdf'.format(url.replace('/','_')+"ev")):
                    status = 'good'
                    self.logger.info("Created evidence for  {} in: {}".format(url ,self.paths['paths'][self.mode]['evidences']))
                    return True
                else:
                    return False
                    
            except Exception as e:
                self.logger.error(e)
                print(number)
                number = number + 1
                self.driver.close()
                self.driver.quit()
                self.display.stop()
                self.restart()
        return False
        
    def translate_page(self):

        #clicking on extensions 
        image_path = self.paths['paths'][self.mode]['extensions']+'/test.png'
        img_location = self.pyautogui.locateOnScreen(image_path,confidence=0.75)
        self.logger.info(f"Extension sign location: {img_location}")
        image_location_point = self.pyautogui.center(img_location)
        x, y = image_location_point
        self.pyautogui.click(x, y)
        time.sleep(2)

        #clicking on translator icon 
        image_path2 = self.paths['paths'][self.mode]['extensions']+'/gg_translate.png'
        ext_location = self.pyautogui.locateOnScreen(image_path2,confidence=0.9)
        self.logger.info(f"Translator sign location: {ext_location}")
        image_location_point = self.pyautogui.center(ext_location)
        x, y = image_location_point
        self.pyautogui.click(x, y)
        time.sleep(3)

        #clicking on 'translate page'
        image_path = self.paths['paths'][self.mode]['extensions']+'/translate-page.png'
        img_location = self.pyautogui.locateOnScreen(image_path, confidence=0.8)
        self.logger.info(f"Translate page location: {img_location}")
        image_location_point = self.pyautogui.center(img_location)
        x, y = image_location_point
        self.pyautogui.position(x,y)
        time.sleep(2)
        self.pyautogui.click(x, y)  
        time.sleep(5)
        self.logger.info("Page translated")


    def text_to_pdf(self,text, filename):

        a4_width_mm = 210
        pt_to_mm = 0.35
        fontsize_pt = 4
        fontsize_mm = fontsize_pt * pt_to_mm
        margin_bottom_mm = 10
        character_width_mm = 7 * pt_to_mm
        width_text = a4_width_mm / character_width_mm
        pdf = fpdf.FPDF(orientation='P', unit='mm', format='A4')
        pdf.set_auto_page_break(True, margin=margin_bottom_mm)
        pdf.add_page()
        pdf.add_font('Liberation', '', '/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf', uni=True)
        pdf.set_font('Liberation', '',4)
        splitted = text.split('\n', 6)
        for line in splitted:
            lines = textwrap.wrap(line, 170)
            if len(lines) == 0:
                pdf.ln()
            for wrap in lines:
                pdf.cell(0, fontsize_mm, wrap, ln=1)
        pdf.output(filename)


    def make_basic_ev(self,url,number):

        self.logger.info("Creating evidence for {}".format(url))

        self.random_wait()
        self.driver.get(url)
        
        self.random_wait()

        try:
            self.translate_page()
        except Exception as e:
            self.logger.error(e)
        try:
            self.translate_page()
        except Exception as e:
            self.logger.error(e)
                                                        
        self.driver.execute_script("window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' })")
        self.random_wait()
        self.driver.execute_script("window.scrollTo({ top: 0, behavior: 'smooth' })")
        self.random_wait()
        self.driver.execute_script("window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' })")
        self.random_wait()
        self.driver.execute_script("window.scrollTo({ top: 0, behavior: 'smooth' })")

        if url.find("coinmarketcap") > -1:
            self.driver.find_element_by_class_name("nameSection") #Check if page opened correctly - find header
            try:
                self.driver.find_element_by_xpath('//*[@id="__next"]/div[1]/div/div[2]/div/div[1]/div[2]/div/div[5]/div/div[3]/div[2]/button').click()
            except Exception as e:
                self.logger.info(e)

        elif url.find("isthiscoinascam") > -1:
            self.driver.find_element_by_class_name("card-body") #Check if page opened correctly - find header
            try:
                self.driver.find_element_by_link_text('Contracts').click()
            except Exception as e:
                print(e)
            self.driver.find_element_by_id("contractTab")

        #Window print and rename file
        try:
            self.driver.execute_script("window.print();")
            self.random_wait()
            file_name = next(os.walk(self.paths['paths'][self.mode]['merge']), (None, None, []))[2][0]
            os.rename(self.paths['paths'][self.mode]['merge'] + '/' + file_name, self.paths['paths'][self.mode]['merge']+ '/page.pdf')
        except:
            pass
        self.logger.info("MAKING SOURCE CODE")
        self.make_source_code(url,"source_code.pdf")
        return number+1

    def make_source_code(self,url,filename):
        if self.driver.current_url != url:
            self.driver.get(url)
            self.random_wait()
        self.logger.info(self.driver.current_url)   
        html = self.driver.page_source

        intro = f'''Source code\n
        Source: {url}\n
        Date: {datetime.datetime.now()}\n\n'''
        full_ev = intro + html
        with open(os.path.join(self.paths['paths'][self.mode]['tmp'],filename), 'w') as f:
            f.write(full_ev)
        file = open(os.path.join(self.paths['paths'][self.mode]['tmp'],filename))
        text = file.read()
        file.close()
        print(os.path.join(self.paths['paths'][self.mode]['merge'],filename))
        self.text_to_pdf(text, os.path.join(self.paths['paths'][self.mode]['merge'],filename))
        time.sleep(3)
        self.driver.execute_script('window.print();')

    def make_pfp_ev(self):

        image_path = self.paths['paths'][self.mode]['extensions']+'/test.png'
        img_location = self.pyautogui.locateOnScreen(image_path,confidence=0.75)
        self.logger.info(img_location)
        image_location_point = self.pyautogui.center(img_location)
        x, y = image_location_point
        self.pyautogui.click(x, y)
        time.sleep(2)
        image_path2 = self.paths['paths'][self.mode]['extensions']+'/clickIt.png'
        ext_location = self.pyautogui.locateOnScreen(image_path2,confidence=0.75)
        self.logger.info(ext_location)
        image_location_point = self.pyautogui.center(ext_location)
        x, y = image_location_point
        self.pyautogui.click(x, y)
        time.sleep(5)
        self.driver.switch_to.frame(self.driver.find_element_by_id("pf-core"))
        self.driver.find_element_by_class_name("pf-sprite").click()


    def merge_ev(self,folder,file_out):
        
        files = os.listdir(folder)
        if len(files) < 3: 
            time.sleep(7)
            files = os.listdir(folder)
        try:
            files.remove('page.pdf')
            files.remove('source_code.pdf')
        except Exception as e:
            self.logger.error(e)


        merger = PdfFileMerger(strict=False)
        try:
            merger.append("{}/page.pdf".format(folder))
        except:
            pass
        try:
            merger.append("{}/{}".format(folder, files[0]))
        except:
            pass
        merger.append("{}/source_code.pdf".format(folder))
        merger.write("{}/{}.pdf".format(self.paths['paths'][self.mode]['evidences'],file_out))

            
