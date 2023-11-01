from ..opensnp import GeneticDataToDataFrameConverter, OpenSNPDownloader, OpenSNPAggregator, AlleleTableGenerator
from ..common import Options
from ..ncbi import *

import os
import pandas as pd

class DNAAnalyzer:
    def __init__(self):
        self._genetic_data_reader = GeneticDataToDataFrameConverter()
        self._opensnp_downloader = OpenSNPDownloader()
        self._opensnp_aggregator = OpenSNPAggregator()
        self._allele_table_generator = AlleleTableGenerator()
        self._ncbi_dataframe_generator = NCBIDataFrameGenerator()
        self._citations_dataframe_generator = CitationsDataframeGenerator()

        self._options = Options()

    def get_cached_merge_dataframe(self, filename):
        file_location = os.path.join(self._options.output_cache_folder(filename), 'raw_merged.parquet')
        if os.path.exists(file_location):
            return pd.read_parquet(file_location)
        self._opensnp_downloader.download_opensnp_data()
        self._opensnp_aggregator.aggregate_data()
        allele_table = self._allele_table_generator.get_allele_table()
        dna = self._genetic_data_reader.read_data(filename)
        merged_dna = dna.merge(allele_table, on=['rsid','chromosome','alleles'], how='left')
        merged_dna.to_parquet(file_location)
        return merged_dna

    def analyze(self, filename):
        merged_dna = self.get_cached_merge_dataframe(filename)
        citations_dataframe = self._citations_dataframe_generator.get_dataframe()
        studied_rsids = citations_dataframe[['rsid']].drop_duplicates()
        merged_dna_data_with_studied_rsids = merged_dna.merge(studied_rsids, on=['rsid'], how='inner')
        ncbi_data = self._ncbi_dataframe_generator.get_dataframe_of_data(merged_dna_data_with_studied_rsids)
        dna_ncbi_augmented = merged_dna.merge(ncbi_data, on=['rsid'], how='inner')

        pass