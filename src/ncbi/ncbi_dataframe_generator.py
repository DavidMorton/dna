from ..common import Options
from .ncbi_data_downloader import NCBIDataDownloader
import os, json
import pandas as pd
from tqdm import trange
from dependency_injector.wiring import Provide

class NCBIDataFrameGenerator:
    def __init__(self,
                 options:Options = Provide['Options'],
                 ncbi_data_downloader:NCBIDataDownloader = Provide['NCBIDataDownloader']):
        self._options = options
        self._ncbi_data_downloader = ncbi_data_downloader

    def get_gene_names(self, json_data, gene_ids):
        genes_found = {gene['id']:gene['locus'] for all_ann in json_data['primary_snapshot_data']['allele_annotations'] for ass_ann in all_ann['assembly_annotation'] for gene in ass_ann['genes']}
        gene_ids = [int(gene_id) for gene_id in gene_ids]
        gene_names = {v for k,v in genes_found.items() if k in gene_ids}
        return ', '.join(gene_names)
    
    def _get_genes(self, assembly_annotations):
        gene_locus = ', '.join([x['locus'] for assembly_annotation in assembly_annotations for x in assembly_annotation['genes']])
        gene_names = ', '.join([x['name'] for assembly_annotation in assembly_annotations for x in assembly_annotation['genes']])
        return gene_locus, gene_names

    def _get_variant_description(self, deleted, inserted):
        if deleted == inserted:
            return 'std','std',deleted, inserted
        if deleted.startswith(inserted):
            del_value = deleted[len(inserted):]
            return 'del', 'del' + del_value, del_value, ''
        
        elif inserted.startswith(deleted):
            new_alleles = inserted[len(deleted):]
            if deleted.endswith(new_alleles):
                return 'dup', 'dup' + new_alleles, '', new_alleles
            else:
                return 'ins', 'ins' + new_alleles, '', new_alleles
        elif (len(deleted) == 1) and (len(inserted) == 1):
            return 'snv', f'{deleted}>{inserted}', deleted, inserted
        return 'unknown', 'unknown', deleted, inserted

    def _process_single_frequency(self, frequency):
        # hgvs = {h['hgvs'] for h in rsdata['primary_snapshot_data']['allele_annotations'][0]['assembly_annotation'][0]['genes'][0]['rnas']}
        # {(allele['allele']['spdi']['deleted_sequence'], allele['allele']['spdi']['inserted_sequence']) for placement in rsdata['primary_snapshot_data']['placements_with_allele'] for allele in placement['alleles'] if allele['hgvs'] in hgvs}
        observation = frequency['observation']
        allele_count = frequency['allele_count']
        total_count = frequency['total_count']
        variant_descriptions = self._get_variant_description(observation['deleted_sequence'], observation['inserted_sequence'])
        return variant_descriptions + (allele_count, total_count)

    def _duplication_reduction(self, frequency_table):
        if len(frequency_table) < 2:
            return frequency_table
        
        variant_types = frequency_table['variant_type'].drop_duplicates().tolist()

        if ('ins' in variant_types) and ('dup' in variant_types):
            if len(frequency_table[['inserted','deleted']].drop_duplicates()) == 1:
                counts = frequency_table.groupby(['inserted','deleted'])[['allele_count','total_count']].sum().reset_index(drop=False)
                frequency_table = frequency_table[frequency_table['variant_type'] == 'dup'].copy().drop(columns=['allele_count','total_count'])
                frequency_table = frequency_table.merge(counts, on=['inserted','deleted'], how='inner').reset_index(drop=True)
                return frequency_table
            
        return frequency_table
    
    def _merge_standard(self, frequency_table):
        value_counts = frequency_table['variant_type'].value_counts()
        if 'std' not in value_counts:
            return frequency_table
        
        num_std = value_counts['std']
        if num_std == 1:
            return frequency_table
        
        lengths = frequency_table.apply(lambda x: len(x['deleted']), axis=1)
        frequency_table['lengths'] = lengths
        short_text = frequency_table.sort_values(by='lengths').iloc[0]['deleted']
        frequency_table.loc[frequency_table.apply(lambda x: short_text in x.deleted, axis=1), ['deleted','inserted']] = short_text
        frequency_table = frequency_table.groupby(['variant_type','description','deleted','inserted'])[['allele_count','total_count']].sum().reset_index(drop=False)

        return frequency_table

    def _process_frequencies(self, frequencies):
        frequency_details = [self._process_single_frequency(frequency) for frequency in frequencies]
        frequency_df = pd.DataFrame(frequency_details, columns=['variant_type','description','deleted','inserted','allele_count','total_count'])
        frequency_table = frequency_df.groupby(by=['variant_type','description','deleted','inserted'])[['allele_count','total_count']].sum().reset_index(drop=False)
        frequency_table = self._duplication_reduction(frequency_table)
        frequency_table = self._merge_standard(frequency_table)
        frequency_table['observed_frequency'] = frequency_table['allele_count'] / frequency_table['total_count']
        return frequency_table
    
    def _process_clinical(self, clinical):
        disease_names = ', '.join(clinical['disease_names'])
        clinical_significance = ', '.join(clinical['clinical_significance'])
        return disease_names, clinical_significance

    def _process_clinicals(self, clinicals):
        diseases = ', '.join({disease_name for clinical in clinicals for disease_name in clinical['disease_names'] if disease_name not in ['not provided']})
        clinical_significances = ', '.join({sig for clinical in clinicals for sig in clinical['clinical_significances']})
        return diseases, clinical_significances

    def _process_allele_annotation(self, allele_annotation):
        if len(allele_annotation['frequency']) == 0:
            return None
        
        frequency_table = self._process_frequencies(allele_annotation['frequency'])
        diseases, significances = self._process_clinicals(allele_annotation['clinical'])
        gene_locus, gene_name = self._get_genes(allele_annotation['assembly_annotation'])
        submission_count = len(allele_annotation['submissions'])

        frequency_table['diseases'] = diseases
        frequency_table['significance'] = significances
        frequency_table['submissions'] = submission_count
        frequency_table['gene_locus'] = gene_locus
        frequency_table['gene_name'] = gene_name
        return frequency_table
    
    def _process_allele_annotations(self, allele_annotations):
        dataframes = [self._process_allele_annotation(annotation) for annotation in allele_annotations]
        dataframes = [x for x in dataframes if x is not None]
        if len(dataframes) > 0:
            return pd.concat(dataframes)
        return None
    

    def _get_clinical_implications(self, json_data):
        result = pd.DataFrame({}, columns=['variant_type','description','deleted','inserted','allele_count','total_count','observed_frequency','diseases','significance','submissions'])

        if 'primary_snapshot_data' not in json_data:
            return result
        
        primary_snapshot_data = json_data['primary_snapshot_data']
        if 'allele_annotations' not in primary_snapshot_data:
            return result
        
        allele_annotations = primary_snapshot_data['allele_annotations']
        if len(allele_annotations) == 0:
            return result
        
        return self._process_allele_annotations(allele_annotations)

    def _get_rsid_json_file(self, rsid):
        json_filename = os.path.join(self._options.ncbi_data_cache, f'{rsid}.json')
        with open(json_filename, 'r') as f:
            json_data = json.loads(f.read())
        return json_data

    def _get_data_for_single_rsid(self, rsid):
        json_data = self._get_rsid_json_file(rsid)

        if 'merged_snapshot_data' in json_data:
            merged_snapshot_data = json_data['merged_snapshot_data']
            if 'merged_into' in merged_snapshot_data:
                merged_into = merged_snapshot_data['merged_into']
                if len(merged_into) > 0:
                    rsid_merged = f'rs{merged_into[0]}'
                    self._ncbi_data_downloader.download_individual_ncbi_data(rsid_merged)
                    return self._get_data_for_single_rsid(rsid_merged)

        clinvar = self._get_clinical_implications(json_data)

        return clinvar
    
    def _regenerate_dataframe(self, merged_dna = None):
        print('Regenerating NIH data on genomes...')
        files = [x for x in os.listdir(self._options.ncbi_data_cache) if x.endswith('.json')]

        if merged_dna is not None:
            requested_rsids = merged_dna['rsid'].tolist()
            files = [x for x in files if x.replace('.json','') in requested_rsids]

        result_list = []
        for file,_ in zip(files, trange(len(files) - 1)):
            rsid = file.replace('.json', '')
            rsid_data = self._get_data_for_single_rsid(rsid)

            if rsid_data is None:
                continue

            rsid_data['rsid'] = rsid
            result_list.append(rsid_data)

        dataframe = pd.concat(result_list)
        intcolumns = ['submissions','total_count','allele_count']
        floatcolumns = ['observed_frequency']
        stringcolumns = [x for x in dataframe.columns if x not in (intcolumns + floatcolumns)]
        dataframe[stringcolumns] = dataframe[stringcolumns].astype(str)
        dataframe[intcolumns] = dataframe[intcolumns].fillna(0).astype(int)
        dataframe[floatcolumns] = dataframe[floatcolumns].fillna(0.0).astype(float)
        return dataframe

    def get_dataframe_of_data(self, merged_dna, allow_download=True, force_regenerate_dataframe=False):
        new_data_found = self._ncbi_data_downloader.download_ncbi_data(merged_dna, allow_download)

        path = self._options.ncbi_dataframe_parquet
        if os.path.exists(path) and (not force_regenerate_dataframe):
            existing_data = pd.read_parquet(path)
            merged_subset = merged_dna[~merged_dna['rsid'].isin(existing_data['rsid'])]
            if (len(merged_subset) > 0) and (not force_regenerate_dataframe):
                added_data = self._regenerate_dataframe(merged_subset)
                if len(added_data) > 0:
                    existing_data = pd.concat([existing_data, added_data])
                    existing_data.to_parquet(path)
                return existing_data
            
            if not new_data_found:
                return existing_data
        
        dataframe = self._regenerate_dataframe(merged_dna)

        dataframe.to_parquet(path)

        return dataframe
        

        

        
        
