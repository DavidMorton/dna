from ..common import Options
import os
import pandas as pd

class OpenSNPProgress:
    def __init__(self):
        self._options = Options()
        self._df:pd.DataFrame = None

    @property
    def _dataframe_path(self):
        return self._options.opensnp_progress_dataframe_path
    
    @property
    def _raw_data_path(self):
        return self._options.opensnp_raw_data
    
    def _get_file_list(self):
        result:list[str] = []
        for f in os.listdir(self._raw_data_path):
            result.append(os.path.join(self._raw_data_path, f))
        return result
    
    def commit_changes(self):
        self.df.to_parquet(self._dataframe_path)

    def _initialize_progress_dataframe(self):
        if not os.path.exists(self._dataframe_path):
            file_list = self._get_file_list()
            file_list = [os.path.basename(x) for x in file_list]
            file_list_df = pd.DataFrame(file_list, columns=['filename'])
            file_list_df['processed'] = False
            file_list_df['errored'] = False
            file_list_df['out_file'] = None
            file_list_df = file_list_df[file_list_df['filename'].str.startswith('user')]
            file_list_df['user'] = file_list_df['filename'].apply(lambda x: x.split('_')[0].replace('user', '')).astype(int)
            file_list_df = file_list_df.sort_values(by='user').reset_index(drop=True)
            file_list_df.to_parquet(self._dataframe_path)
        
        

        result = pd.read_parquet(self._dataframe_path)
        filenames = os.listdir(self._options.combined_user_data_subset_directory)
        result.loc[~result['out_file'].isin(filenames), ['processed','errored','out_file']] = [False, False, None]
        result.to_parquet(self._dataframe_path)
        return result
    
    def get_next_out_filename(self):
        addx = 0
        existing_files = self.df['out_file'].unique()
        out_file = f'combined_{(addx):04.0f}.parquet'
        while out_file in existing_files:
            addx+=1
            out_file = f'combined_{(addx):04.0f}.parquet'
        return out_file

    def save_aggregated_subset(self, out_df:pd.DataFrame, out_file:str):
        if out_df is not None:
            out_df.to_parquet(os.path.join(self._options.combined_user_data_subset_directory, out_file))
        self.commit_changes()

    @property
    def df(self):
        if self._df is None:
            self._df = self._initialize_progress_dataframe()
        return self._df

    def mark_processed(self, filename, errored, out_file):
        filename = os.path.basename(filename)
        self.df.loc[self.df['filename'] == filename, ['processed','errored','out_file']] = [True, errored, out_file]

    def get_unprocessed_user_data(self):
        unprocessed_user_list = self.df.loc[self.df['processed'] == False, 'user'].drop_duplicates().tolist()
        if len(unprocessed_user_list) == 0:
            return None
        unprocessed_user = unprocessed_user_list[0]
        unprocessed_files = self.df.loc[self.df['user'] == unprocessed_user, 'filename'].tolist()
        unprocessed_files_full_paths = [os.path.join(self._options.opensnp_raw_data, x) for x in unprocessed_files]
        return unprocessed_files_full_paths


