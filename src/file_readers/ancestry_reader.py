from .genetic_file_reader import GeneticFileReader
import pandas as pd

class AncestryReader(GeneticFileReader):
    def _get_file_data_impl(self, filename):
        skiplines = self.get_line_skips(filename)
        if skiplines is None:
            return None
        skiplines+=1

        user_id = self.get_user_id(filename)
        result = pd.read_csv(filename, sep='\t', skiprows=skiplines, engine='pyarrow', header=None, dtype={'rsid':str,'chromosome':str,'position':int,'allele1':str,'allele2':str}, names=['rsid','chromosome','position','allele1','allele2'])
        result['alleles'] = result['allele1'].replace('None', '') + result['allele2'].replace('None','')
        result = result.drop(columns=['allele1','allele2'])
        result['user'] = user_id
        result['source'] = 'ancestry'
        result = self.post_process_genome_data(result)
        return result
