import os

from functions.folder_functions import create_tmp_directory, get_tmp_directory_path, \
    create_questionnaire_name_folder_in_tmp_directory, clear_tmp_directory
from functions.zip_functions import create_zip


def test_create_zip():
    clear_tmp_directory()
    create_tmp_directory()
    tmp_folder = get_tmp_directory_path()
    create_questionnaire_name_folder_in_tmp_directory("blah")
    create_zip(os.path.join(tmp_folder, "blah"), f"{tmp_folder}/zip_file")
    assert os.path.isfile(tmp_folder + "/zip_file.zip")
