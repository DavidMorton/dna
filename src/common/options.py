import os

class Options:
    @property
    def data_folder(self):
        data_folder = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '.data'))
        os.makedirs(data_folder, exist_ok=True)
        return data_folder
    
    @property
    def public_data_folder(self):
        data_folder = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'data'))
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
    
    @property
    def ncbi_data_cache(self):
        result = os.path.join(self.data_folder, 'raw_ncbi_data')
        os.makedirs(result, exist_ok=True)
        return result
    
    @property
    def ncbi_dataframe_parquet(self):
        result = os.path.join(self.data_folder, 'ncbi_data.parquet')
        return result
    
    @property
    def public_ncbi_dataframe_parquet(self):
        result = os.path.join(self.public_data_folder, 'ncbi_data.parquet')
        return result
    
    @property
    def citations_text_file(self):
        result = os.path.join(self.data_folder, 'var_citations.txt')
        return result
    
    @property
    def citations_parquet_file(self):
        result = os.path.join(self.data_folder, 'var_citations.parquet')
        return result
    
    def output_cache_folder(self, filename):
        result = os.path.join(self.data_folder,'output',os.path.basename(filename).split('.')[0])
        os.makedirs(result, exist_ok=True)
        return result