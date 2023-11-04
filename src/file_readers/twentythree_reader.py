from .genetic_file_reader import GeneticFileReader
import pandas as pd

class TwentyThreeReader(GeneticFileReader):
    def _get_file_data_impl(self, filename):
        skiplines = self.get_line_skips(filename)
        if skiplines is None:
            raise Exception('Could not find a proper number of lines to skip.')

        result = pd.read_csv(filename, skiprows=skiplines, sep='\t', engine='pyarrow', header=None, dtype={'rsid':str,'chromosome':str,'position':int,'alleles':str}, names=['rsid','chromosome','position','alleles'])
        user_id = self.get_user_id(filename)
        result['user'] = user_id
        result['source'] = '23andme'
        result = self.post_process_genome_data(result)
        return result
