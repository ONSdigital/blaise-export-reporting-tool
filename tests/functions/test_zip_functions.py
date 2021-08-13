import io
import zipfile

from functions.zip_functions import create_zip, zip_data_group


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
