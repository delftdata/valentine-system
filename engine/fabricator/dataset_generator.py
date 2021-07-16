import os
import configparser
import pandas as pd
from shutil import copyfile
import json
from io import BytesIO
from minio import Minio

from .vertical_transformations import split as vertical_split
from .horizontal_transformations import split as horizontal_split
from .horizontal_transformations import overlap2sources
from .dataset import Dataset
from .add_noise_schema import approximate_column_names
from random import choice, randint


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
    target_dir = os.path.join(path, name)

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

    f1 = open(os.path.join(target_dir, name) + '_source.json', 'w')
    f3 = open(os.path.join(target_dir, name) + '_target.json', 'w')
    f5 = open(os.path.join(target_dir, name) + '_mapping.json', 'w')

    json_schema = {}
    for key in target1.schema.keys():
        json_schema[target1.schema[key][0]] = {
            'type': target1.schema[key][1]
        }
    json.dump(json_schema, f1, indent=4)

    target1.data = pd.DataFrame(
        target1.data.values,
        columns=json_schema.keys()
    )

    target1.data.to_csv(os.path.join(target_dir, name)+'_source.csv', mode='w', header=True, index=False)

    json_schema = {}
    for key in target2.schema.keys():
        json_schema[target2.schema[key][0]] = {
            'type': target2.schema[key][1]
        }
    json.dump(json_schema, f3, indent=4)

    target2.data = pd.DataFrame(
        target2.data.values,
        columns=json_schema.keys()
    )
    
    target2.data.to_csv(os.path.join(target_dir, name)+'_target.csv', mode='w', header=True, index=False)

    matches = []
    for i in mapping.keys():
        matches.append(mapping[i])
    json_matches = {'matches': matches}
    json.dump(json_matches, f5, indent=4)

    copyfile(cfgfile, os.path.join(target_dir, cfgfile.split('\\')[-1]))

    f1.close()
    f3.close()
    f5.close()


def write_files_to_minio(target1: Dataset, target2: Dataset, mapping: dict, dir_name: str, client: Minio,
                         bucket: str, group_name: str):

    json_schema = {}
    for key in target1.schema.keys():
        json_schema[target1.schema[key][0]] = {
            'type': target1.schema[key][1]
        }
    
    json1_bytes = json.dumps(json_schema, indent=4).encode('utf-8')
    json1_buffer = BytesIO(json1_bytes)

    client.put_object(bucket, group_name + os.path.sep + dir_name + os.path.sep +
                      dir_name.split(os.path.sep)[1] + '_source_schema.json',
                      data=json1_buffer, length=len(json1_bytes))

    target1.data = pd.DataFrame(
        target1.data.values,
        columns=json_schema.keys()
    )

    target1_bytes = target1.data.to_csv().encode('utf-8')
    target1_buffer = BytesIO(target1_bytes)
    client.put_object(bucket, group_name + os.path.sep + dir_name + os.path.sep +
                      dir_name.split(os.path.sep)[1] + '_source.csv',
                      data=target1_buffer, length=len(target1_bytes))

    json_schema = {}
    for key in target2.schema.keys():
        json_schema[target2.schema[key][0]] = {
            'type': target2.schema[key][1]
        }
    json2_bytes = json.dumps(json_schema, indent=4).encode('utf-8')
    json2_buffer = BytesIO(json2_bytes)

    client.put_object(bucket, group_name + os.path.sep + dir_name + os.path.sep +
                      dir_name.split(os.path.sep)[1] + '_target_schema.json',
                      data=json2_buffer, length=len(json2_bytes))

    target2.data = pd.DataFrame(
        target2.data.values,
        columns=json_schema.keys()
    )
    
    target2_bytes = target2.data.to_csv().encode('utf-8')
    target2_buffer = BytesIO(target2_bytes)
    client.put_object(bucket, group_name + os.path.sep + dir_name + os.path.sep +
                      dir_name.split(os.path.sep)[1] + '_target.csv',
                      data=target2_buffer, length=len(target2_bytes))

    matches = []
    for i in mapping.keys():
        matches.append(mapping[i])
    json_matches = {'matches': matches}
    
    matches_bytes = json.dumps(json_matches, indent=4).encode('utf-8')
    matches_buffer = BytesIO(matches_bytes)
    client.put_object(bucket, group_name + os.path.sep + dir_name + os.path.sep +
                      dir_name.split(os.path.sep)[1] + '_matches.json',
                      data=matches_buffer, length=len(matches_bytes))


def valentine_fabricator(scenario: str, parameters: list[bool], no_pairs: int, in_data: pd.DataFrame,
                         in_schema: dict, group_name: str, filename: str, client: Minio, bucket: str):

    in_schema = {int(k): v for k, v in in_schema.items()}
    in_dataset = Dataset(in_schema, in_data)

    overlaps = [0.0, 0.5, 1.0]
    prcs = [30, 50, 70]
    approx_prcs = [10, 20, 30]
    columns = [0.3, 0.5, 0.7]

    noisy_instances, noisy_schemata, verbatim_instances, verbatim_schemata = parameters

    if scenario == 'Joinable':

        columns = [0.0, 0.3, 0.5, 0.7]

        pairs_ver = no_pairs // 2
        pairs_both = no_pairs - pairs_ver

        if noisy_schemata:

            pairs_ver_verb = pairs_ver_noisy = pairs_ver // 2
            pairs_both_verb = pairs_both_noisy = pairs_both // 2

            for i in range(pairs_ver_verb):
                target1, target2, common = vertical_split(in_dataset, 0, choice(columns), False, choice(prcs))
                matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                name_pair = scenario + '/' + filename + '_' + str(i+1) + '_joinable_vertical_verbatim_schemata' 
                write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

            for i in range(pairs_ver_noisy):
                target1, target2, common = vertical_split(in_dataset, 0, choice(columns), False, choice(prcs))
                target2 = approximate_column_names(target2, filename, randint(1, 5))
                matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                name_pair = scenario + '/' + filename + '_' + str(i+1) + '_joinable_vertical_noisy_schemata' 
                write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

            for i in range(pairs_both_verb):
                target1, target2, common = vertical_split(in_dataset, 0, choice(columns), False, choice(prcs))
                matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                target1, target2 = overlap2sources(target1, target2, 0.5, False)
                name_pair = scenario + '/' + filename + '_' + str(i+1) + '_joinable_both_verbatim_schemata' 
                write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

            for i in range(pairs_both_noisy):
                target1, target2, common = vertical_split(in_dataset, 0, choice(columns), False, choice(prcs))
                target2 = approximate_column_names(target2, filename, randint(1, 5))
                matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                target1, target2 = overlap2sources(target1, target2, 0.5, False)
                name_pair = scenario + '/' + filename + '_' + str(i+1) + '_joinable_both_noisy_schemata' 
                write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

        else:

            for i in range(pairs_ver):

                target1, target2, common = vertical_split(in_dataset, 0, choice(columns), False, choice(prcs))
                matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                name_pair = scenario + '/' + filename + '_' + str(i+1) + '_joinable_vertical_verbatim_schemata' 
                write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

            for i in range(pairs_both):

                target1, target2, common = vertical_split(in_dataset, 0, choice(columns), False, choice(prcs))
                matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                target1, target2 = overlap2sources(target1, target2, 0.5, False)
                name_pair = scenario + '/' + filename + '_' + str(i+1) + '_joinable_both_verbatim_schemata' 
                write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

    elif scenario == 'Semantically-Joinable':

        columns = [0.0, 0.3, 0.5, 0.7]

        pairs_ver = no_pairs // 2
        pairs_both = no_pairs - pairs_ver

        if noisy_schemata:

            pairs_ver_verb = pairs_ver_noisy = pairs_ver // 2
            pairs_both_verb = pairs_both_noisy = pairs_both // 2

            for i in range(pairs_ver_verb):
                target1, target2, common = vertical_split(in_dataset, 0, choice(columns), True, choice(approx_prcs))
                matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                name_pair = scenario + '/' + filename + '_' + str(i+1) + '_semantically_joinable_vertical_verbatim_schemata' 
                write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

            for i in range(pairs_ver_noisy):
                target1, target2, common = vertical_split(in_dataset, 0, choice(columns), True, choice(approx_prcs))
                target2 = approximate_column_names(target2, filename, randint(1, 5))
                matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                name_pair = scenario + '/' + filename + '_' + str(i+1) + '_semantically_joinable_vertical_noisy_schemata' 
                write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

            for i in range(pairs_both_verb):
                target1, target2, common = vertical_split(in_dataset, 0, choice(columns), True, choice(approx_prcs))
                matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                target1, target2 = overlap2sources(target1, target2, 0.5, False)
                name_pair = scenario + '/' + filename + '_' + str(i+1) + '_semantically_joinable_both_verbatim_schemata' 
                write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

            for i in range(pairs_both_noisy):
                target1, target2, common = vertical_split(in_dataset, 0, choice(columns), True, choice(approx_prcs))
                target2 = approximate_column_names(target2, filename, randint(1, 5))
                matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                target1, target2 = overlap2sources(target1, target2, 0.5, False)
                name_pair = scenario + '/' + filename + '_' + str(i+1) + '_semantically_joinable_both_noisy_schemata' 
                write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

        else:

            for i in range(pairs_ver):

                target1, target2, common = vertical_split(in_dataset, 0, choice(columns), True, choice(approx_prcs))
                matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                name_pair = scenario + '/' + filename + '_' + str(i+1) + '_semantically_joinable_vertical_verbatim_schemata' 
                write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

            for i in range(pairs_both):

                target1, target2, common = vertical_split(in_dataset, 0, choice(columns), True, choice(approx_prcs))
                matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                target1, target2 = overlap2sources(target1, target2, 0.5, False)
                name_pair = scenario + '/' + filename + '_' + str(i+1) + '_semantically_joinable_both_verbatim_schemata' 
                write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

    elif scenario == 'Unionable':

        if verbatim_schemata and not noisy_schemata:

            if verbatim_instances and not noisy_instances:

                for i in range(no_pairs):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__verbatim_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)
            elif verbatim_instances and noisy_instances:

                pairs_ver = no_pairs // 2
                pairs_noi = no_pairs - pairs_ver

                for i in range(pairs_ver):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__verbatim_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

                for i in range(pairs_noi):

                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__verbatim_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)
            else:

                for i in range(no_pairs):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__verbatim_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

        elif verbatim_schemata and noisy_schemata:

            if verbatim_instances and not noisy_instances:

                pairs_ver = no_pairs // 2
                pairs_noi = no_pairs - pairs_ver 

                for i in range(pairs_ver):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__verbatim_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

                for i in range(pairs_noi):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__noisy_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)    

            elif verbatim_instances and noisy_instances:

                pairs_ver_ver = no_pairs // 4
                pairs_ver_noi = no_pairs // 4
                pairs_noi_ver = no_pairs // 4
                pairs_noi_noi = no_pairs - (pairs_ver_ver + pairs_ver_noi + pairs_noi_ver)

                for i in range(pairs_ver_ver):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__verbatim_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

                for i in range(pairs_ver_noi):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__verbatim_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

                for i in range(pairs_noi_ver):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__noisy_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name) 

                for i in range(pairs_noi_noi):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__noisy_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)    
                
            else:

                pairs_ver = no_pairs // 2
                pairs_noi = no_pairs - pairs_ver 

                for i in range(pairs_ver):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__verbatim_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

                for i in range(pairs_noi):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__noisy_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)    

        else:
            if verbatim_instances and not noisy_instances:
                for i in range(no_pairs):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__noisy_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name) 
                
            elif verbatim_instances and noisy_instances:

                pairs_ver = no_pairs // 2
                pairs_noi = no_pairs - pairs_ver 
                
                for i in range(pairs_ver):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__noisy_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name) 

                for i in range(pairs_noi):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__noisy_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)    
                
            else:

                for i in range(no_pairs):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_unionable__noisy_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)    

    else:

        if verbatim_schemata and not noisy_schemata:

            if verbatim_instances and not noisy_instances:

                for i in range(no_pairs):
                    target1, target2, common = vertical_split(in_dataset, 0, choice(columns), False, False)
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable_verbatim_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)
            elif verbatim_instances and noisy_instances:

                pairs_ver = no_pairs // 2
                pairs_noi = no_pairs - pairs_ver

                for i in range(pairs_ver):
                    target1, target2, common = vertical_split(in_dataset, 0, choice(columns), False, False)
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__verbatim_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

                for i in range(pairs_noi):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__verbatim_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)
            else:

                for i in range(no_pairs):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__verbatim_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

        elif verbatim_schemata and noisy_schemata:

            if verbatim_instances and not noisy_instances:

                pairs_ver = no_pairs // 2
                pairs_noi = no_pairs - pairs_ver 

                for i in range(pairs_ver):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__verbatim_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

                for i in range(pairs_noi):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__noisy_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)    

            elif verbatim_instances and noisy_instances:

                pairs_ver_ver = no_pairs // 4
                pairs_ver_noi = no_pairs // 4
                pairs_noi_ver = no_pairs // 4
                pairs_noi_noi = no_pairs - (pairs_ver_ver + pairs_ver_noi + pairs_noi_ver)

                for i in range(pairs_ver_ver):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__verbatim_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

                for i in range(pairs_ver_noi):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__verbatim_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

                for i in range(pairs_noi_ver):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__noisy_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name) 

                for i in range(pairs_noi_noi):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__noisy_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)    
                
            else:

                pairs_ver = no_pairs // 2
                pairs_noi = no_pairs - pairs_ver 

                for i in range(pairs_ver):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__verbatim_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)

                for i in range(pairs_noi):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__noisy_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)    

        else:

            if verbatim_instances and not noisy_instances:
                for i in range(no_pairs):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__noisy_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name) 
                
            elif verbatim_instances and noisy_instances:

                pairs_ver = no_pairs // 2
                pairs_noi = no_pairs - pairs_ver 
                
                for i in range(pairs_ver):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, False)
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__noisy_schemata_verbatim_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name) 

                for i in range(pairs_noi):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__noisy_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)    
                
            else:

                for i in range(no_pairs):
                    target1, target2, common = horizontal_split(in_dataset, choice(overlaps), False, True, choice(approx_prcs))
                    target2 = approximate_column_names(target2, filename, randint(1, 5))
                    matches = create_mappings('source', 'target', target1.schema, target2.schema, common)
                    target1, target2 = overlap2sources(target1, target2, 0.0, False)
                    name_pair = scenario + '/' + filename + '_' + str(i+1) + '_view_unionable__noisy_schemata_noisy_instances' 
                    write_files_to_minio(target1, target2, matches, name_pair, client, bucket, group_name)   
