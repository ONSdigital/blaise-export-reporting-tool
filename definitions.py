import os

if os.getenv("K_SERVICE", "") == "bert-deliver-mi-hub-reports":
    ROOT_DIR = "/"
else:
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
