import os
from .options import Options

class OpenSNPDownloader:
    def __init__(self):
        self._options = Options()

    def download_opensnp_data(self):
        target_path = os.path.join(self._options.data_folder, 'opensnp')
        if os.path.exists(target_path):
            return target_path

        