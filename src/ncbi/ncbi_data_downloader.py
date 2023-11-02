from ..common import Options
import os
import requests
from tqdm import trange
from datetime import datetime, timedelta
import time
from dependency_injector.wiring import Provide

class NCBIDataDownloader:
    def __init__(self, options:Options = Provide['Options']):
        self._options = options

    def download_individual_ncbi_data(self, rsid, prev_start_time=None):

        targetpath = os.path.join(self._options.ncbi_data_cache, f'{rsid}.json')
        if os.path.exists(targetpath):
            return False, prev_start_time
        try:
            now = datetime.now()
            if (prev_start_time is not None) and ((prev_start_time + timedelta(seconds=1)) > now):
                diff:datetime = (prev_start_time + timedelta(seconds=1))
                sleeptime = abs((now - diff).total_seconds())
                #time.sleep(0.001)
                time.sleep(sleeptime)
            
            rsid_number = int(rsid.replace('rs', ''))
            start_time = datetime.now()
            response = requests.get(f'https://api.ncbi.nlm.nih.gov/variation/v0/refsnp/{rsid_number}', timeout=5)
            if response.status_code == 200:
                content = response.content.decode()
                with open(targetpath, 'w') as f:
                    f.write(content)
                    return True, start_time
        except:
            print('unable to download', rsid)
            return False, datetime.now()


    def download_ncbi_data(self, merged_dna, allow_download = True):
        if not allow_download:
            return False
        # don't do any downloading between 9 and 4:00
        if 9 < datetime.now().hour < 15:
            print('No new bulk download during work hours')
            return True
        
        print('Downloading NCBI Data')
        genotypes = set(merged_dna.loc[merged_dna['rsid'].str.startswith('rs'), 'rsid'].to_list())
        already_cached = [x.replace('.json', '') for x in os.listdir(self._options.ncbi_data_cache)]
        for x in already_cached:
            if x in genotypes:
                genotypes.remove(x)
        
        new_data = False
        
        start_time = datetime.now() - timedelta(seconds=10)
        t_range = trange(len(genotypes) - 1)
        for genotype,t in zip(genotypes, t_range):
            result, start_time = self.download_individual_ncbi_data(genotype, start_time)
            new_data = result or new_data
            t_range.refresh()

        return new_data

