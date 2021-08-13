import os
import zipfile
import io


def create_zip(files):
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
