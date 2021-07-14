import os
import zipfile


def prepare_zip(dir_path, zip_name):
    new_file = zip_name + ".zip"
    # creating zip file with write mode
    zip = zipfile.ZipFile(new_file, "w", zipfile.ZIP_DEFLATED)
    # Walk through the files in a directory
    for dir_path, dir_names, files in os.walk(dir_path):
        f_path = dir_path.replace(dir_path, "")
        f_path = f_path and f_path + os.sep
        # Writing each file into the zip
        for file in files:
            zip.write(os.path.join(dir_path, file), f_path + file)
    zip.close()
    print(f"File created - {new_file}")
    return new_file
