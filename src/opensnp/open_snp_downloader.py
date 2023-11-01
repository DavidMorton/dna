import os
from ..common import Options
from dependency_injector.wiring import Provide

class OpenSNPDownloader:
    def __init__(self, options:Options = Provide['Options']):
        self._options = options

    def download_opensnp_data(self):
        target_path = os.path.join(self._options.data_folder, 'opensnp')
        if os.path.exists(target_path):
            return target_path

        