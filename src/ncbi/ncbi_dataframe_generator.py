from ..common import Options
from .ncbi_data_downloader import NCBIDataDownloader
import os, json
import pandas as pd
from tqdm import trange
from dependency_injector.wiring import Provide
from dask.distributed import Client, progress

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

    def _process_assembly_annotation(self, assembly_annotation, placement_with_alleles):
        unique_deleted_and_inserted = {(x['allele']['spdi']['deleted_sequence'],x['allele']['spdi']['inserted_sequence'],x['hgvs'].endswith('=')) for p in placement_with_alleles if p['is_ptlp'] for x in p['alleles']}
        variant_descriptions = [self._get_variant_description(d,i) + (std,) for d,i,std in unique_deleted_and_inserted]
        return variant_descriptions
    
    def _process_assembly_annotations(self, assembly_annotations, placements_with_alleles):
        result = [x for annotation in assembly_annotations for x in self._process_assembly_annotation(annotation, placements_with_alleles)]
        return pd.DataFrame(result, columns=['variant_type','description','deleted','inserted','is_ref'])

    def _process_frequencies(self, frequencies):
        frequency_details = [self._process_single_frequency(frequency) for frequency in frequencies]
        frequency_df = pd.DataFrame(frequency_details, columns=['variant_type','description','deleted','inserted','allele_count','total_count'])
        frequency_table = frequency_df.groupby(by=['variant_type','description','deleted','inserted'])[['allele_count','total_count']].sum().reset_index(drop=False)
        frequency_table = self._duplication_reduction(frequency_table)
        frequency_table = self._merge_standard(frequency_table)
        return frequency_table
    
    def _process_clinical(self, clinical):
        disease_names = ', '.join(clinical['disease_names'])
        clinical_significance = ', '.join(clinical['clinical_significance'])
        return disease_names, clinical_significance

    def _process_clinicals(self, clinicals):
        diseases = ', '.join({disease_name for clinical in clinicals for disease_name in clinical['disease_names'] if disease_name not in ['not provided', 'not specified']})
        clinical_significances = ', '.join({sig for clinical in clinicals for sig in clinical['clinical_significances']})
        return diseases, clinical_significances

    def _process_allele_annotation(self, allele_annotation, placement_with_alleles):
        assembly_annotations = self._process_assembly_annotations(allele_annotation['assembly_annotation'], placement_with_alleles)
        if len(allele_annotation['frequency']) > 0:
            frequency_table = self._process_frequencies(allele_annotation['frequency'])
            annotations_w_frequency = assembly_annotations.merge(frequency_table, on=['variant_type','description','deleted','inserted'], how='right')
        else:
            annotations_w_frequency = assembly_annotations.copy()
            annotations_w_frequency[['allele_count','total_count']] = [0,0]

        return annotations_w_frequency
    
    def _process_allele_annotations(self, allele_annotations, placements_with_allele):
        dataframes = [self._process_allele_annotation(annotation, placements_with_allele) for annotation in allele_annotations]
        dataframes = [x for x in dataframes if x is not None]
        if len(dataframes) > 0:
            result = pd.concat(dataframes).drop_duplicates()
            result = result.groupby(['variant_type','description','deleted','inserted','is_ref'])[['allele_count','total_count']].sum().reset_index(drop=False)
            result['observed_frequency'] = (result['allele_count'] / result['total_count']).fillna(0.0)

            diseases = ', '.join({disease_name for allele_annotation in allele_annotations for clinical in allele_annotation['clinical'] for disease_name in clinical['disease_names'] if disease_name not in ['not_provided','not_specified']})
            significances = ', '.join({sig for allele_annotation in allele_annotations for clinical in allele_annotation['clinical'] for sig in clinical['clinical_significances']})
            loci = ', '.join({gene['locus'] for allele_annotation in allele_annotations for assembly_annotation in allele_annotation['assembly_annotation'] for gene in assembly_annotation['genes']})
            names = ', '.join({gene['name'] for allele_annotation in allele_annotations for assembly_annotation in allele_annotation['assembly_annotation'] for gene in assembly_annotation['genes']})
            submission_count = len([sub for allele_annotation in allele_annotations for sub in allele_annotation['submissions']])

            result[['diseases','significances','submissions','gene_locus','gene_name']] = [diseases,significances,submission_count,loci,names]
            result['is_ref'] = result['is_ref'].fillna(result['description'] == 'std')
            result.loc[result['is_ref'],['diseases','significances']] = ''
            result = result.drop(columns='is_ref')
            return result

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
        
        placements_with_allele = primary_snapshot_data['placements_with_allele']
        if len(placements_with_allele) == 0:
            return result
        
        return self._process_allele_annotations(allele_annotations, placements_with_allele)

    def _get_rsid_json_file(self, rsid):
        json_filename = os.path.join(self._options.ncbi_data_cache, f'{rsid}.json')
        if not os.path.exists(json_filename):
            self._ncbi_data_downloader.download_individual_ncbi_data(rsid)
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
    
    def _get_rsid_data(self, rsid):
        rsid_data = self._get_data_for_single_rsid(rsid)

        if (rsid_data is None) or (len(rsid_data) == 0):
            return None
        
        rsid_data['rsid'] = rsid
        return rsid_data

    def _regenerate_dataframe(self, merged_dna = None):
        print('Regenerating NIH data on genomes...')
        files = [x for x in os.listdir(self._options.ncbi_data_cache) if x.endswith('.json')]

        if merged_dna is not None:
            requested_rsids = merged_dna['rsid'].tolist()
            missing_rsids = [x for x in requested_rsids if x.replace('.json','') not in files]

        result_list = []
        
        #rsids = [file.replace('.json','') for file in files]

        client = Client()
        futures = client.map(self._get_rsid_data, missing_rsids)
        fut = client.compute(futures)
        progress(fut)
        
        results = [r.result() for r in futures]
        result_list = [r for r in results if r is not None]
        
        if len(result_list) == 0:
            return None
        
        dataframe = pd.concat(result_list)
        intcolumns = ['submissions','total_count','allele_count']
        floatcolumns = ['observed_frequency']
        stringcolumns = [x for x in dataframe.columns if x not in (intcolumns + floatcolumns)]
        dataframe[stringcolumns] = dataframe[stringcolumns].astype(str)
        dataframe[intcolumns] = dataframe[intcolumns].fillna(0).astype(int)
        dataframe[floatcolumns] = dataframe[floatcolumns].fillna(0.0).astype(float)
        return dataframe

    def get_dataframe_of_data(self, merged_dna, allow_download=True, force_regenerate_dataframe=False):
        public_path = self._options.public_ncbi_dataframe_parquet
        if os.path.exists(public_path) and (not force_regenerate_dataframe):
            print('This repo comes with a default, pre-processed version of the NCBI data, generated sometime')
            print('around November 2023. Newer studies may not be reflected in this file.')
            result = ''
            while result.upper() not in ['Y','N']:
                result = input('Would you like to use this file instead of generating a new one? (Y/N)? ')
            if result.upper() == 'Y':
                return pd.read_parquet(self._options.public_ncbi_dataframe_parquet)
            
        new_data_found = self._ncbi_data_downloader.download_ncbi_data(merged_dna, allow_download)

        path = self._options.ncbi_dataframe_parquet
        if os.path.exists(path) and (not force_regenerate_dataframe):
            existing_data = pd.read_parquet(path)
            merged_subset = merged_dna[~merged_dna['rsid'].isin(existing_data['rsid'])]
            if (len(merged_subset) > 0) and (not force_regenerate_dataframe):
                added_data = self._regenerate_dataframe(merged_subset)
                if (added_data is not None) and (len(added_data) > 0):
                    existing_data = pd.concat([existing_data, added_data])
                    existing_data.to_parquet(path)
                return existing_data
            
            if not new_data_found:
                return existing_data
        
        dataframe = self._regenerate_dataframe(merged_dna)

        dataframe.to_parquet(path)

        return dataframe
        

        

        
        
