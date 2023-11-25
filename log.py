# -*- coding: utf-8 -*-

import logging
import inspect
import os
from logging.handlers import TimedRotatingFileHandler

class CustomLogger(logging.Logger):
	def info(self, msg, *args, **kwargs):
		current_function_name = inspect.currentframe().f_back.f_code.co_name
		current_file_name = os.path.basename(inspect.currentframe().f_back.f_code.co_filename)
		line_number = inspect.currentframe().f_back.f_lineno
		msg = "\n    " + str(msg)
		msg = f"({current_function_name} in {current_file_name}:{line_number}) {msg}"
		super().info(msg, *args, **kwargs)

	def warning(self, msg, *args, **kwargs):
		current_function_name = inspect.currentframe().f_back.f_code.co_name        
		current_file_name = os.path.basename(inspect.currentframe().f_back.f_code.co_filename)
		line_number = inspect.currentframe().f_back.f_lineno        
		msg = "\n    " + str(msg)
		msg = f"({current_function_name} in {current_file_name}:{line_number}) {msg}"
		super().warning(msg, *args, **kwargs)

	def error(self, msg, *args, **kwargs):
		current_function_name = inspect.currentframe().f_back.f_code.co_name        
		current_file_name = os.path.basename(inspect.currentframe().f_back.f_code.co_filename)
		line_number = inspect.currentframe().f_back.f_lineno        
		msg = "\n    " + str(msg)
		msg = f"({current_function_name} in {current_file_name}:{line_number}) {msg}"
		super().error(msg, *args, **kwargs)   

import datetime
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
log_folder = os.path.join("logs", current_date)

if not os.path.exists(log_folder):
	os.makedirs(log_folder)

_logger_nm = "xqfollower"

logger = CustomLogger(_logger_nm)
logger.setLevel(logging.INFO)

fmt = logging.Formatter(
"%(asctime)s [%(levelname)s]: %(message)s"
)

log_handler = TimedRotatingFileHandler(filename=os.path.join(log_folder, f'{_logger_nm}.log'), when='midnight', interval=1, backupCount=90)
log_handler.setFormatter(fmt)
logger.addHandler(log_handler)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(fmt)
logger.addHandler(console)
def log_wrapper(*args):
    logger.info(" ".join(str(arg) for arg in args))

