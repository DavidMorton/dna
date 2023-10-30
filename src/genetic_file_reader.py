import os
import pandas as pd
from abc import ABC, abstractmethod

class GeneticFileReader(ABC):
    def get_user_id(self, filename):
        return int(os.path.basename(filename).split('_')[0].replace('user', ''))
    
    def post_process_genome_data(self, result):
        result['position'] = pd.to_numeric(result['position'], errors='coerce')
        result = result.dropna().copy()
        result['user'] = pd.to_numeric(result['user'], errors='coerce')
        result = result.dropna().copy()
        return result

    @abstractmethod
    def get_file_data(self, filename):
        pass