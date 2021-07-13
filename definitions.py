import os

if os.getenv('GAE_ENV', '').startswith('standard'):
    ROOT_DIR = "/"
else:
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
