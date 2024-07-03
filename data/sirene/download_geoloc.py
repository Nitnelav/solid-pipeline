# https://files.data.gouv.fr/insee-sirene-geo/GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.zip

import urllib.request

def configure(context):
    pass

def execute(context):

    url = "https://files.data.gouv.fr/insee-sirene-geo/GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.zip"

    with context.progress(label = "Downloading GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8 ...", minimum_interval=5) as progress:
        def progress_hook(count, block_size, total_size):
            percent = int(count * block_size * 100 / total_size)
            progress.tracker.total = total_size
            progress.update(block_size)

        urllib.request.urlretrieve(url, "%s/GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.zip" % context.path(), reporthook=progress_hook)

    return "GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.zip"
