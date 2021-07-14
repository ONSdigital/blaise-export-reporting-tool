import os
import zipfile


def prepare_zip(dir_path, zip_name):
    new_zip_file = zip_name + ".zip"
    zip_file = zipfile.ZipFile(new_zip_file, "w", zipfile.ZIP_DEFLATED)
    for dir_path, dir_names, files in os.walk(dir_path):
        f_path = dir_path.replace(dir_path, "")
        f_path = f_path and f_path + os.sep
        for file in files:
            zip_file.write(os.path.join(dir_path, file), f_path + file)
    zip_file.close()
    print(f"File created - {new_zip_file}")
    return new_zip_file
