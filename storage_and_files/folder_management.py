import os

from definitions import ROOT_DIR


def clear_directory(folder_dir):
    for root, dirs, files in os.walk(folder_dir):
        for file in files:
            os.remove(os.path.join(root, file))

    folders = os.listdir(folder_dir)
    for folder in folders:
        os.rmdir(os.path.join(folder_dir, folder))


def clear_tmp_directory():
    tmp_folder = get_tmp_directory_path()
    clear_directory(tmp_folder)


def create_tmp_directory():
    print(ROOT_DIR)
    tmp_folder = os.path.join(ROOT_DIR, "tmp")
    try:
        os.mkdir(tmp_folder)
    except FileExistsError:
        print(f"tmp Folder already exists")


def create_folder_in_tmp_directory(questionnaire_name):
    tmp_folder = get_tmp_directory_path()
    try:
        os.mkdir(os.path.join(tmp_folder, questionnaire_name))
    except FileExistsError:
        print(f"Folder {questionnaire_name} exists")


def get_tmp_directory_path():
    tmp_folder = os.path.join(ROOT_DIR, "tmp")
    return tmp_folder
