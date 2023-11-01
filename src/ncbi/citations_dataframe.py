from ..common import Options
import os, requests
import pandas as pd

from dependency_injector.wiring import Provide

class CitationsDataframeGenerator:
    def __init__(self, options:Options = Provide['Options']):
        self._options = options

    def _download_citations(self):
        if not os.path.exists(self._options.citations_text_file):
            url = 'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/var_citations.txt'
            response = requests.get(url)
            if response.status_code == 200:
                with open(self._options.citations_text_file, 'w') as f:
                    f.write(response.content.decode())

    def get_dataframe(self):
        if os.path.exists(self._options.citations_parquet_file):
            result = pd.read_parquet(self._options.citations_parquet_file)
            return result
        
        self._download_citations()
        result = pd.read_csv(self._options.citations_text_file, delimiter='\t')
        result = result[~result['rs'].isna()]
        result['rs'] = 'rs' + result['rs'].astype(int).astype(str)
        result = result.rename(columns={'rs':'rsid'})
        result.to_parquet(self._options.citations_parquet_file)
        return result

