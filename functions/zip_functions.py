import io
import zipfile


def create_zip(files):
    memory_zip = io.BytesIO()
    with zipfile.ZipFile(memory_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zip:
        for file in files:
            zip.writestr(file["filename"], file["content"])
    zip_bytes = memory_zip.getvalue()
    memory_zip.close()
    return zip_bytes


def zip_data_group(zip_data_grouped_by_questionnaire, questionnaire_name):
    if questionnaire_name in zip_data_grouped_by_questionnaire.keys():
        return zip_data_grouped_by_questionnaire[questionnaire_name]
    return {}
