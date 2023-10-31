import os

class Options:
    @property
    def data_folder(self):
        data_folder = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '.data'))
        os.makedirs(data_folder, exist_ok=True)
        return data_folder
    
    @property
    def opensnp_raw_data(self):
        return os.path.join(self.data_folder, 'opensnp')
    
    @property
    def opensnp_progress_dataframe_path(self):
        return os.path.join(self.data_folder, 'opensnp_progress.parquet')
    
    @property
    def combined_user_data_subset_directory(self):
        result = os.path.join(self.data_folder, 'combined_subsets')
        os.makedirs(result, exist_ok=True)
        return result
    
    @property
    def frequency_raw_combined_parquet(self):
        result = os.path.join(self.data_folder, 'frequency_raw_combined.parquet')
        return result
    
    @property
    def frequency_combined_refined(self):
        result = os.path.join(self.data_folder, 'frequency_refined.parquet')
        return result