# https://www.insee.fr/fr/statistiques/fichier/7708995/reference_IRIS_geo2023.zip

import urllib.request
import zipfile

def configure(context):
    pass

def execute(context):

    url = "https://www.insee.fr/fr/statistiques/fichier/7708995/reference_IRIS_geo2023.zip"


    with context.progress(label = "Downloading reference_IRIS_geo2023.zip ...", minimum_interval=5) as progress:
        def progress_hook(count, block_size, total_size):
            percent = int(count * block_size * 100 / total_size)
            progress.tracker.total = total_size
            progress.update(block_size)

        zip_file, headers = urllib.request.urlretrieve(url, "%s/reference_IRIS_geo2023.zip" % context.path(), reporthook=progress_hook)
 
    with zipfile.ZipFile(zip_file) as zip:
        zip.extract("reference_IRIS_geo2023.xlsx", context.path())

    return "reference_IRIS_geo2023.xlsx" 
