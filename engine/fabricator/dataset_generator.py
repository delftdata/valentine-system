import sys, os
import configparser
import pandas as pd
from shutil import copyfile
import json
from io import BytesIO
from minio import Minio

from .configuration import Configuration
from .vertical_transformations import split as vertical_split
from .horizontal_transformations import split as horizontal_split
from .dataset import Dataset
from .add_noise_schema import approximate_column_names
from random import choice



def read_config(cfgfile: str):
    cfg = configparser.ConfigParser()
    cfg.read(cfgfile)


def read_schema(infile: str):
    f = open(infile, 'r')
    lines = f.read().splitlines()
    return {a: lines[a].split(',') for a in range(len(lines))}


def read_data(infile: str):
    df = pd.read_csv(infile, skiprows=1, header=None, delimiter=',')
    return df


def create_mappings(schema_name1: str, schema_name2: str, schema1: dict, schema2: dict, common_columns):
    mapping = {}
    for i in common_columns:
        mapping[i] = {
            'source_table': schema_name1,
            'source_column': schema1[i][0],
            'target_table': schema_name2,
            'target_column': schema2[i][0]
        }
    return mapping


def create_target_files(target1: Dataset, target2: Dataset, path: str, name: str, mapping: dict, cfgfile: str):
    target_dir = os.path.join(path,name)

    answer = 'y'

    if os.path.isdir(target_dir):
        print("Directory exists! Do you want to overwrite it and its files? Type 'y' or 'n'")
        answer = input()

    if answer == 'n':
        print('A new directory is created with the name %s_new!' % target_dir)
        target_dir = target_dir+'_new'
        try:
            os.mkdir(target_dir)
        except OSError:
            print("Creation of the directory %s failed" % target_dir)
        else:
            print("Successfully created the directory %s " % target_dir)

    else:
        try:
            os.mkdir(target_dir)
        except OSError:
            print("Directory %s is overwritten " % target_dir)
        else:
            print("Successfully created the directory %s " % target_dir)

    f1 = open(os.path.join(target_dir,name)+'_source.json', 'w')
    f3 = open(os.path.join(target_dir,name)+'_target.json', 'w')
    f5 = open(os.path.join(target_dir, name)+'_mapping.json', 'w')

    json_schema = {}
    for key in target1.schema.keys():
        json_schema[target1.schema[key][0]] = {
            'type': target1.schema[key][1]
        }
    json.dump(json_schema, f1, indent= 4)

    target1.data = pd.DataFrame(
        target1.data.values,
        columns=json_schema.keys()
    )

    target1.data.to_csv(os.path.join(target_dir,name)+'_source.csv', mode= 'w', header=True, index=False)

    json_schema = {}
    for key in target2.schema.keys():
        json_schema[target2.schema[key][0]] = {
            'type': target2.schema[key][1]
        }
    json.dump(json_schema, f3, indent= 4)

    target2.data = pd.DataFrame(
        target2.data.values,
        columns=json_schema.keys()
    )
    
    target2.data.to_csv(os.path.join(target_dir,name)+'_target.csv', mode= 'w', header=True, index=False)

    matches = []
    for i in mapping.keys():
        matches.append(mapping[i])
    json_matches = {'matches': matches}
    json.dump(json_matches, f5, indent= 4)

    copyfile(cfgfile, os.path.join(target_dir, cfgfile.split('\\')[-1]))

    f1.close()
    f3.close()
    f5.close()


def write_files_to_minio(target1: Dataset, target2: Dataset, mapping: dict, dir_name: str, client: Minio, bucket: str):
    

    json_schema = {}
    for key in target1.schema.keys():
        json_schema[target1.schema[key][0]] = {
            'type': target1.schema[key][1]
        }
    #json.dump(json_schema, f1, indent= 4)

    target1.data = pd.DataFrame(
        target1.data.values,
        columns=json_schema.keys()
    )

    target1_bytes = target1.data.to_csv().encode('utf-8')
    target1_buffer = BytesIO(target1_bytes)
    client.put_object(bucket, dir_name+'_source.csv', data=target1_buffer, length=len(target1_bytes))

    json_schema = {}
    for key in target2.schema.keys():
        json_schema[target2.schema[key][0]] = {
            'type': target2.schema[key][1]
        }
    #json.dump(json_schema, f3, indent= 4)

    target2.data = pd.DataFrame(
        target2.data.values,
        columns=json_schema.keys()
    )
    
    target2_bytes = target2.data.to_csv().encode('utf-8')
    target2_buffer = BytesIO(target2_bytes)
    client.put_object(bucket, dir_name+'_target.csv', data=target2_buffer, length=len(target2_bytes))

    #matches = []
    #for i in mapping.keys():
    #    matches.append(mapping[i])
    #json_matches = {'matches': matches}
    #json.dump(json_matches, f5, indent= 4)




def transformer(argv: list):

    cfgfile = argv[1]
    config = Configuration(cfgfile)

    schema = read_schema(config.source_schema)
    data = read_data(config.source_data)
    target_path = config.output_dir
    target_name = config.target_names
    source = Dataset(schema, data)
    overlap = config.overlap/100.
    rand = config.random_overlap
    pk = config.primary_key
    com_col = config.columns/100.
    rand_col = config.col_split_random
    approx = config.approx
    prc = config.prc
    approx_prc = config.approx_prc
    approx_columns = config.approx_columns
    approx_columns_type = config.approx_columns_type


    if config.vertical_split and not config.horizontal_split:
        target1, target2, common = vertical_transformations.split(source, pk, com_col, rand_col, approx, prc, approx_prc)
        if approx_columns:
            target2 = add_noise_schema.approximate_column_names(target2, target_name, approx_columns_type)
        matches = create_mappings(target_name+'_source', target_name+'_target', target1.schema, target2.schema, common)
        create_target_files(target1, target2, target_path, target_name, matches,cfgfile)
        print("2 Target schemas created by splitting vertically!")
    elif config.horizontal_split and not config.vertical_split:
        target1, target2, common = horizontal_transformations.split(source, overlap, rand, approx, prc, approx_prc)
        if approx_columns:
            target2 = add_noise_schema.approximate_column_names(target2, target_name, approx_columns_type)
        matches = create_mappings(target_name + '_source', target_name + '_target', target1.schema, target2.schema, common)
        create_target_files(target1, target2, target_path, target_name, matches,cfgfile)
        print("2 Target schemas created by splitting horizontally!")
    elif config.horizontal_split and config.vertical_split:
        target1, target2, common = vertical_transformations.split(source, pk, com_col, rand_col, approx, prc, approx_prc)
        if approx_columns:
            target2 = add_noise_schema.approximate_column_names(target2, target_name, approx_columns_type)
        matches = create_mappings(target_name + '_source', target_name + '_target', target1.schema, target2.schema, common)
        target1, target2 = horizontal_transformations.overlap2sources(target1, target2, overlap, rand)
        create_target_files(target1, target2, target_path, target_name, matches,cfgfile)
    else:
        print('wrong_configuration')


def valentine_fabricator(scenario: str, parameters: list[bool], no_pairs: int, in_data: pd.DataFrame, in_schema: dict, filename: str, client: Minio, bucket: str):

    
    in_schema = {int(k): v for k,v in in_schema.items()}
    in_dataset = Dataset(in_schema, in_data)

    overlaps = [0.0,0.5, 1.0]
    prcs = [30,50,70]
    approx_prcs = [10,20,30]
    columns = [0.3,0.5,0.7]

    noisy_instances, noisy_schemata, verbatim_instances, verbatim_schemata = parameters

    if scenario == 'Joinable':

        pairs_ver = no_pairs // 2
        pairs_both = no_pairs - pairs_ver


        if noisy_schemata:

            pairs_ver_verb = pairs_ver_noisy = pairs_ver // 2
            pairs_both_verb = pairs_both_noisy = pairs_both // 2

        else:

            for i in range(pairs_ver):

                target1, target2, common = vertical_split(in_dataset, 0, choice(columns), False, False, choice(prcs), None)
                matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                name_pair = filename + '_' + str(i+1) + '_joinable_vertical' 
                write_files_to_minio(target1, target2, matches, name_pair, client, bucket)
    



if __name__ == "__main__":
    argv = sys.argv
    if len(argv) <= 1:
        print("TODO: Help menu / usage")
    else:
        transformer(argv)
