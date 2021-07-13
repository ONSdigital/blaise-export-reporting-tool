import os

if os.getenv("FUNCTION_NAME", "") == "bert-deliver-mi-hub-reports":
    ROOT_DIR = "/"
else:
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
