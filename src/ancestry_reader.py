from .genetic_file_reader import GeneticFileReader
import pandas as pd

class AncestryReader(GeneticFileReader):
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
            skiplines+=1

            user_id = self.get_user_id(filename)
            result = pd.read_csv(filename, sep='\t', skiprows=skiplines, engine='pyarrow', header=None, dtype={'rsid':str,'chromosome':str,'position':int,'allele1':str,'allele2':str}, names=['rsid','chromosome','position','allele1','allele2'])
            result['alleles'] = result['allele1'] + result['allele2']
            result = result.drop(columns=['allele1','allele2'])
            result['user'] = user_id
            result['source'] = 'ancestry'
            result = self.post_process_genome_data(result)
            return result
        except:
            return None
