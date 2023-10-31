from ..common import Options
import os
import pandas as pd
from tqdm import trange

class AlleleTableGenerator:
    def __init__(self):
        self._options = Options()

    def _get_alleles(self, filename):
        allele_filename = os.path.join(self._options.data_folder, 'alleles_' + os.path.basename(filename))
        if os.path.exists(allele_filename):
            return pd.read_parquet(allele_filename)
        
        df = pd.read_parquet(filename)
        counts = df.groupby(['rsid', 'alleles']).count().reset_index(drop=False)
        counts = counts[['rsid','alleles','chromosome']].rename(columns={'chromosome':'instances'})
        counts.to_parquet(allele_filename)
        return counts

    def build_alleles_table(self):
        result = None
        files = os.listdir(self._options.combined_user_data_subset_directory)
        files.sort()
        for filename in files:
            filename = os.path.join(self._options.combined_user_data_subset_directory, filename)
            counts = self._get_alleles(filename)
            if result is None:
                result = counts
            else:
                result = pd.concat([result, counts]).groupby(['rsid','alleles']).sum().reset_index(drop=False)

        return result


    def _get_combined_genotype_frequencies(self):

        if os.path.exists(self._options.frequency_raw_combined_parquet):
            return pd.read_parquet(self._options.frequency_raw_combined_parquet)
        
        filelist = os.listdir(self._options.combined_user_data_subset_directory)
        filelist = [x for x in filelist if x.endswith('.parquet')]
        filelist.sort()

        df = pd.read_parquet(os.path.join(self._options.combined_user_data_subset_directory, filelist[0]))
        for f, _ in zip(filelist[1:], trange(len(filelist[1:]))):
            df2 = pd.read_parquet(os.path.join(self._options.combined_user_data_subset_directory,f))
            cols = ['rsid','chromosome','position','alleles','source']
            df = pd.concat((df, df2))
            df = df.groupby(cols)['count'].sum().reset_index()

        try:
            df.to_parquet(self._options.frequency_raw_combined_parquet)
        except:
            print('could not save the file')
            pass

        return df
    
    def get_allele_table(self):
        outfile = self._options.frequency_combined_refined

        if os.path.exists(outfile):
            return pd.read_parquet(outfile)
        
        df = self._get_combined_genotype_frequencies()
        
        reduced = df[df['rsid'].str.startswith('rs') | df['rsid'].str.startswith('i')][['rsid','chromosome','alleles','count']]
        reduced['alleles'] = reduced['alleles'].str.replace('None','')
        reduced = reduced.groupby(['rsid','chromosome','alleles'])['count'].sum()
        reduced = reduced.reset_index(drop=False)
        reduced.to_parquet(outfile)
        return reduced
    