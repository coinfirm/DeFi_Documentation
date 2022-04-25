import logging
from datetime import datetime
import os

def logger_setup(name, path):

        if os.path.isdir(path + '/' + datetime.today().strftime('%Y-%m-%d')):
            pass
        else:
            os.mkdir(path + '/' + datetime.today().strftime('%Y-%m-%d'))

        handler_format = logging.Formatter("%(asctime)s %(levelname)s %(lineno)d:%(filename)s(%(process)d) - %(message)s")

        handler = logging.StreamHandler()
        handler.setFormatter(handler_format)

        logger = logging.getLogger(name)
        logger.setLevel(level=logging.INFO)     
        logger.addHandler(handler)

        save_path = path + '/' +  datetime.today().strftime('%Y-%m-%d')

        logging.basicConfig(filename=save_path + '/' + name,
                            filemode='w',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.INFO)

        return logger