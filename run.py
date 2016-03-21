from log.logger import setup_logging
from application.process import process
import sys
import importlib
import datetime


if len(sys.argv) == 2:
    d = sys.argv[1]
else:
    d = datetime.now().strftime('%Y-%m-%d')

cfg = 'Config'
c = getattr(importlib.import_module('config'), cfg)
config = {}

for key in dir(c):
    if key.isupper():
        config[key] = getattr(c, key)

setup_logging(config)
if process(config, d):
    exit(0)
exit(1)

