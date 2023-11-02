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
    
    def get_clinical_implications(self, json_data):
        result = (None, None, None)
        if 'primary_snapshot_data' not in json_data:
            return result
        
        primary_snapshot_data = json_data['primary_snapshot_data']
        if 'allele_annotations' not in primary_snapshot_data:
            return result
        
        allele_annotations = primary_snapshot_data['allele_annotations']
        if len(allele_annotations) == 0:
            return result
        
        clinical_data = []
        for x in allele_annotations:
            clinical_data = clinical_data + x['clinical']

        if len(clinical_data) == 0:
            return result
        
        disease_names = set()
        gene_ids = set()
        results = []
        for x in clinical_data:
            disease_name = x['disease_names'][0]
            if disease_name not in ['not provided', 'not specified']:
                results.append({
                    'disease_name': x['disease_names'][0],
                    'gene': self.get_gene_names(json_data, x['gene_ids']),
                    'significance': x['clinical_significances'][0]
                })
            # for disease in x['disease_names']:
            #     disease_names.add(disease)    
            # for gene in x['gene_ids']:
            #     gene_name = self.get_gene_name(json_data, gene)
            #     gene_ids.add(gene_name)
        if len(results) > 0:
            results = list(pd.DataFrame(results).drop_duplicates().T.to_dict().values())
            disease = ', '.join([x['disease_name'] for x in results])
            gene = ', '.join({x['gene'] for x in results})
            significance = ', '.join({x['significance'] for x in results})
            results = (disease, gene, significance)
        else:
            return (None, None, None)
        

        return results

    def _get_variant_description(self, deleted, inserted):
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
        return 'unknown', deleted, inserted
    
    def _get_data_for_single_rsid(self, rsid):
        default_result = ('unknown','','')
        clinvar = (None, None, None)
        json_filename = os.path.join(self._options.ncbi_data_cache, f'{rsid}.json')
        with open(json_filename, 'r') as f:
            json_data = json.loads(f.read())

        if 'merged_snapshot_data' in json_data:
            merged_snapshot_data = json_data['merged_snapshot_data']
            if 'merged_into' in merged_snapshot_data:
                merged_into = merged_snapshot_data['merged_into']
                if len(merged_into) > 0:
                    rsid_merged = f'rs{merged_into[0]}'
                    self._ncbi_data_downloader.download_individual_ncbi_data(rsid_merged)
                    return self._get_data_for_single_rsid(rsid_merged)

        if 'primary_snapshot_data' not in json_data:
            return [default_result + clinvar]
        primary_snapshot_data = json_data['primary_snapshot_data']
        support = primary_snapshot_data['support'][-1]
        revision = support['revision_added']
        value = support['id']['value']
        if value.startswith('ss'):
            value = value.replace('ss', '')
        present_obs_movements = json_data['present_obs_movements']

        obs_key = 'allele_in_cur_release'
        unique_observations = [(x[obs_key]['deleted_sequence'], x[obs_key]['inserted_sequence']) for x in present_obs_movements if x[obs_key]['deleted_sequence'] != x[obs_key]['inserted_sequence']]
        definitions = pd.DataFrame(unique_observations).drop_duplicates().values.tolist()

        if len(definitions) == 0:
            return [default_result + clinvar]
        
        unique_observations = [self._get_variant_description(x[0], x[1]) for x in definitions]
        
        clinvar = self.get_clinical_implications(json_data)

        result = [x + clinvar for x in unique_observations]
        return result

    def _regenerate_dataframe(self, merged_dna = None):
        print('Regenerating NIH data on genomes...')
        files = [x for x in os.listdir(self._options.ncbi_data_cache) if x.endswith('.json')]

        result_list = []
        for file,_ in zip(files, trange(len(files) - 1)):
            rsid = file.replace('.json', '')
            alleles = '--'
            if merged_dna is not None:
                user_values = merged_dna[merged_dna['rsid'] == rsid]
                if len(user_values) > 0:
                    row = user_values.iloc[0]
                    alleles = row['alleles']

            rsid_data = self._get_data_for_single_rsid(rsid)
            for rsid_datum in rsid_data:
                result_list.append((rsid,) + rsid_datum)
        
        return pd.DataFrame(result_list, columns=['rsid','variation_type','description','deleted','inserted','disease','gene','significance'])

    def get_dataframe_of_data(self, merged_dna, allow_download=True, force_regenerate_dataframe=False):
        new_data_found = self._ncbi_data_downloader.download_ncbi_data(merged_dna, allow_download)

        path = self._options.ncbi_dataframe_parquet
        if os.path.exists(path) and (not force_regenerate_dataframe):
            existing_data = pd.read_parquet(path)
            if not new_data_found:
                return existing_data
        
        dataframe = self._regenerate_dataframe(merged_dna)
        dataframe.to_parquet(path)

        return dataframe
        

        

        
        
