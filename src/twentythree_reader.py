from .genetic_file_reader import GeneticFileReader
import pandas as pd
import numpy as np

class TwentyThreeReader(GeneticFileReader):
    def get_file_data(self, filename):
        try:
            skiplines = 0
            with open(filename, 'r') as f:
                line = f.readline()
                if line.startswith('\x00'):
                    return None
                while line.startswith('#'):
                    skiplines+=1
                    line = f.readline()

            result = pd.read_csv(filename, skiprows=skiplines, sep='\t', engine='pyarrow', header=None, dtype={'rsid':str,'chromosome':str,'position':int,'alleles':str}, names=['rsid','chromosome','position','alleles'])
            user_id = self.get_user_id(filename)
            result['user'] = user_id
            result['source'] = '23andme'
            result = self.post_process_genome_data(result)
            return result
        except:
            return None
