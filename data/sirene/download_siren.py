# https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip

import urllib.request

def configure(context):
    pass

def execute(context):

    url = "https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip"

    with context.progress(label = "Downloading StockUniteLegale_utf8 ...", minimum_interval=5) as progress:
        def progress_hook(count, block_size, total_size):
            percent = int(count * block_size * 100 / total_size)
            progress.tracker.total = total_size
            progress.update(block_size)

        urllib.request.urlretrieve(url, "%s/StockUniteLegale_utf8.zip" % context.path(), reporthook=progress_hook)

    return "StockUniteLegale_utf8.zip"
