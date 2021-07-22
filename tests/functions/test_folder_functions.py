from functions.folder_functions import *


def test_get_tmp_directory_path():
    tmp_folder = get_tmp_directory_path()
    assert tmp_folder.endswith("tmp")


def test_get_tmp_directory_path_app_engine():
    if ROOT_DIR == "/":
        tmp_folder = get_tmp_directory_path()
        assert tmp_folder == "/tmp"


def test_create_tmp_directory():
    create_tmp_directory()
    tmp_folder = os.path.join(ROOT_DIR, "tmp")
    assert os.path.exists(tmp_folder)


def test_create_questionnaire_name_folder_in_tmp_directory():
    create_questionnaire_name_folder_in_tmp_directory("blah")
    questionnaire_folder = os.path.join(ROOT_DIR, "tmp", "blah")
    assert os.path.exists(questionnaire_folder)


def test_clear_tmp_directory():
    clear_tmp_directory()
    tmp_folder = os.path.join(ROOT_DIR, "tmp")
    assert len(os.listdir(tmp_folder)) == 0
