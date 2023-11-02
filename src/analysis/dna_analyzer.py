from ..opensnp import GeneticDataToDataFrameConverter, OpenSNPDownloader, OpenSNPAggregator, AlleleTableGenerator
from ..common import Options
from ..ncbi import *
import os
import pandas as pd
from dependency_injector.wiring import Provide

class DNAAnalyzer:
    def __init__(self, 
                 genetic_data_reader:GeneticDataToDataFrameConverter = Provide['GeneticDataToDataFrameConverter'],
                 opensnp_downloader:OpenSNPDownloader = Provide['OpenSNPDownloader'],
                 opensnp_aggregator:OpenSNPAggregator = Provide['OpenSNPAggregator'],
                 allele_table_generator:AlleleTableGenerator = Provide['AlleleTableGenerator'],
                 ncbi_dataframe_generator:NCBIDataFrameGenerator = Provide['NCBIDataFrameGenerator'],
                 citations_dataframe_generator:CitationsDataframeGenerator = Provide['CitationsDataframeGenerator'],
                 options:Options = Provide['Options']):
        self._genetic_data_reader = genetic_data_reader
        self._opensnp_downloader = opensnp_downloader
        self._opensnp_aggregator = opensnp_aggregator
        self._allele_table_generator = allele_table_generator
        self._ncbi_dataframe_generator = ncbi_dataframe_generator
        self._citations_dataframe_generator = citations_dataframe_generator
        self._options = options

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
        ncbi_data = self._ncbi_dataframe_generator.get_dataframe_of_data(merged_dna_data_with_studied_rsids, allow_download=True, force_regenerate_dataframe=True)
        dna_ncbi_augmented = merged_dna.merge(ncbi_data, on=['rsid'], how='inner')
        detected_snvs = dna_ncbi_augmented[dna_ncbi_augmented.apply(lambda x:x['inserted'] in x['alleles'], axis=1) & (dna_ncbi_augmented['variant_type'] == 'snv')]
        detected_delinv = dna_ncbi_augmented[(dna_ncbi_augmented['alleles'] == 'DI') & (dna_ncbi_augmented['variant_type'].isin(['del','ins','dup']))]
        dd = dna_ncbi_augmented[(dna_ncbi_augmented['alleles'] == 'DD') & (dna_ncbi_augmented['variant_type'].isin(['std'])) & (dna_ncbi_augmented['deleted'] == '') & (dna_ncbi_augmented['inserted'] == '')]
        ii = dna_ncbi_augmented[(dna_ncbi_augmented['alleles'] == 'II') & (dna_ncbi_augmented['variant_type'].isin(['std'])) & (dna_ncbi_augmented['deleted'] != '') & (dna_ncbi_augmented['inserted'] != '')]
        std = dna_ncbi_augmented[(dna_ncbi_augmented['variant_type'] == 'std') & ((dna_ncbi_augmented['alleles'] == (dna_ncbi_augmented['inserted']*2)) | (dna_ncbi_augmented['alleles'] == dna_ncbi_augmented['inserted']))]

        detected = pd.concat([detected_snvs, detected_delinv, dd, ii, std]).reset_index(drop=True)
        detected = detected.sort_values(by=['chromosome','position'])
        detected.to_excel(os.path.join(self._options.output_cache_folder(filename), f'{os.path.basename(filename).split(".")[0]}_variations.xlsx'))

        pass