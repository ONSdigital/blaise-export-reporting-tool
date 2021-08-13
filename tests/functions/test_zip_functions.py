import os
import zipfile
import io

from functions.folder_functions import (
    create_tmp_directory,
    get_tmp_directory_path,
    create_questionnaire_name_folder_in_tmp_directory,
    clear_tmp_directory,
)
from functions.zip_functions import create_zip, create_zip_mem, zip_group


def test_create_zip():
    clear_tmp_directory()
    create_tmp_directory()
    tmp_folder = get_tmp_directory_path()
    create_questionnaire_name_folder_in_tmp_directory("blah")
    create_zip(os.path.join(tmp_folder, "blah"), f"{tmp_folder}/zip_file")
    assert os.path.isfile(tmp_folder + "/zip_file.zip")


def test_create_zip_mem():
    files = [
        {"filename": "foo.csv", "content": "foobar, baz"},
        {"filename": "bar.csv", "content": "fsh, fwibble"},
    ]
    zipped_bytes = create_zip_mem(files)
    zipped_io = io.BytesIO(zipped_bytes)
    zipped_object = zipfile.ZipFile(zipped_io)
    zip_info = zipped_object.infolist()
    assert len(zip_info) == 2
    assert zip_info[0].filename == "foo.csv"
    assert zip_info[1].filename == "bar.csv"


def test_zip_group_no_group():
    groups = {}
    assert zip_group(groups, "foobar") == {}


def test_zip_group_with_group():
    groups = {"foobar": {"fwibble": "fish"}}
    assert zip_group(groups, "foobar") == {"fwibble": "fish"}
