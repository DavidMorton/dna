
from src import *

class Main():
    def __init__(self):
        self._opensnp_downloader = OpenSNPDownloader()
        self._opensnp_aggregator = OpenSNPAggregator()
        self._options = Options()

    def run_analysis(self, filename:str):
        #if os.path.exists(self._options.opensnp_progress_dataframe_path):
        #    os.remove(self._options.opensnp_progress_dataframe_path)
        
        opensnp_folder = self._opensnp_downloader.download_opensnp_data()
        self._opensnp_aggregator.aggregate_data()


m = Main()
m.run_analysis(None)