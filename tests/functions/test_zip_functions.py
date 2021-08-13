import os
import zipfile
import io

from functions.zip_functions import create_zip, zip_group


def test_create_zip():
    files = [
        {"filename": "foo.csv", "content": "foobar, baz"},
        {"filename": "bar.csv", "content": "fsh, fwibble"},
    ]
    zipped_bytes = create_zip(files)
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
