import os
import pandas as pd
from abc import ABC, abstractmethod

class GeneticFileReader(ABC):
    def get_line_skips(self, filename):
        try:
            skiplines = 0
            with open(filename, 'r') as f:
                line = f.readline()
                if line.startswith('\x00'):
                    return None
                while line.startswith('#'):
                    skiplines+=1
                    line = f.readline()
            return skiplines
        except:
            return None
        
    def get_user_id(self, filename):
        try:
            return int(os.path.basename(filename).split('_')[0].replace('user', ''))
        except:
            return 0
    
    def post_process_genome_data(self, result):
        result['position'] = pd.to_numeric(result['position'], errors='coerce')
        result = result.dropna().copy()
        result['user'] = pd.to_numeric(result['user'], errors='coerce')
        result = result.dropna().copy()
        return result

    @abstractmethod
    def _get_file_data_impl(self, filename):
        pass

    def get_file_data(self, filename, throw_on_error=False):
        if throw_on_error:
            return self._get_file_data_impl(filename)
        else:
            try:
                return self._get_file_data_impl(filename)
            except:
                return None
        