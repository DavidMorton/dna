from .options import Options
from .open_snp_progress import OpenSNPProgress
from .genetic_data_to_dataframe_converter import GeneticDataToDataFrameConverter
import pandas as pd
import os

class OpenSNPAggregator:
    def __init__(self):
        self._options = Options()
        self._opensnp_progress = OpenSNPProgress()
        self._dataframe_converter = GeneticDataToDataFrameConverter()
    
    def aggregate_50_users(self):
        unprocessed_user_files = self._opensnp_progress.get_unprocessed_user_data()
        count = 1
        if unprocessed_user_files is None:
            return False
        
        out_df = None
        out_file = self._opensnp_progress.get_next_out_filename()
        
        while unprocessed_user_files is not None and count < 50:
            
            dataframes = []
            user_df = None
            for file in unprocessed_user_files:
                print(file)
                data = self._dataframe_converter.read_data(file)
                errored = data is None
                if not errored:
                    dataframes.append(data)
                
                self._opensnp_progress.mark_processed(file, errored, out_file)
                for df in dataframes:
                    if user_df is None:             
                        user_df = df
                    else:
                        new_user_data = df[~df['rsid'].isin(user_df['rsid'])]
                        user_df = pd.concat((user_df, new_user_data))
                
            if out_df is None:
                out_df = user_df
            else:
                out_df = pd.concat([out_df, user_df])
            
            count = count + 1
            unprocessed_user_files = self._opensnp_progress.get_unprocessed_user_data()

        if out_df is not None:
            out_df['chromosome'] = out_df['chromosome'].astype(str)
            out_df = out_df[['rsid','chromosome','position','alleles','source']].value_counts().reset_index()

        self._opensnp_progress.save_aggregated_subset(out_df, out_file)

        return True

    def aggregate_data(self):
        while self.aggregate_50_users():
            print('next file')

            
