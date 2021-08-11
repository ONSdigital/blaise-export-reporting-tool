import os
import zipfile
import io


def create_zip(dir_path, zip_file):
    new_zip_file = zip_file + ".zip"
    zip_file = zipfile.ZipFile(new_zip_file, "w", zipfile.ZIP_DEFLATED)
    for dir_path, dir_names, files in os.walk(dir_path):
        f_path = dir_path.replace(dir_path, "")
        f_path = f_path and f_path + os.sep
        for file in files:
            zip_file.write(os.path.join(dir_path, file), f_path + file)
    zip_file.close()
    print(f"File created - {new_zip_file}")
    return new_zip_file


def create_zip_mem(files):
    mem_zip = io.BytesIO()

    with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zip:
        for file in files:
            zip.writestr(file["filename"], file["content"])

    zip_bytes = mem_zip.getvalue()
    mem_zip.close()
    return zip_bytes


def zip_group(zip_groups, group_name):
    if group_name in zip_groups.keys():
        return zip_groups[group_name]
    return {}
