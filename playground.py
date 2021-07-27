# fs = frozenset({('miller2_both_0_1_ac2_av_target', 'CouReID'), ('miller2_both_0_1_ac2_av_source', 'Section ID')})
#
# t = tuple(fs)
# inv_t = (t[1], t[0])
# print(t, inv_t)
#
# d = {(('miller2_both_0_1_ac2_av_source', 'Section ID'), ('miller2_both_0_1_ac2_av_target', 'CouReID')): 0.7976470588235294,
#      (('miller2_both_0_1_ac2_av_source', 'Project number'), ('miller2_both_0_1_ac2_av_target', 'OrganCl')): 0.7867132867132869,
#      (('miller2_both_0_1_ac2_av_source', 'Fund centre ID'), ('miller2_both_0_1_ac2_av_target', 'SecID')): 0.7825386996904025,
#      (('miller2_both_0_1_ac2_av_source', 'Division name'), ('miller2_both_0_1_ac2_av_target', 'OrganCl')): 0.7755656108597286,
#      (('miller2_both_0_1_ac2_av_source', 'Branch ID'), ('miller2_both_0_1_ac2_av_target', 'OrgaID')): 0.76,
#      (('miller2_both_0_1_ac2_av_source', 'Section name'), ('miller2_both_0_1_ac2_av_target', 'OrganCl')): 0.7591880341880342,
#      (('miller2_both_0_1_ac2_av_source', 'Division name'), ('miller2_both_0_1_ac2_av_target', 'OrganS')): 0.7577342047930282,
#      (('miller2_both_0_1_ac2_av_source', 'Branch name'), ('miller2_both_0_1_ac2_av_target', 'OrganCl')): 0.7572649572649572,
#      (('miller2_both_0_1_ac2_av_source', 'Section ID'), ('miller2_both_0_1_ac2_av_target', 'OrgaID')): 0.7540106951871659, (('miller2_both_0_1_ac2_av_source', 'Fund centre ID'), ('miller2_both_0_1_ac2_av_target', 'ContID')): 0.752, (('miller2_both_0_1_ac2_av_source', 'Section ID'), ('miller2_both_0_1_ac2_av_target', 'PrBroCounID')): 0.7511586452762924, (('miller2_both_0_1_ac2_av_source', 'Project number'), ('miller2_both_0_1_ac2_av_target', 'OrganS')): 0.7475935828877005, (('miller2_both_0_1_ac2_av_source', 'Division ID'), ('miller2_both_0_1_ac2_av_target', 'CouReID')): 0.7363636363636363, (('miller2_both_0_1_ac2_av_source', 'Branch name'), ('miller2_both_0_1_ac2_av_target', 'OrganS')): 0.7333333333333334, (('miller2_both_0_1_ac2_av_source', 'Division ID'), ('miller2_both_0_1_ac2_av_target', 'OrgaID')): 0.7333333333333334, (('miller2_both_0_1_ac2_av_source', 'Section name'), ('miller2_both_0_1_ac2_av_target', 'OrganS')): 0.7333333333333334, (('miller2_both_0_1_ac2_av_source', 'Division ID'), ('miller2_both_0_1_ac2_av_target', 'OrganS')): 0.7192156862745098, (('miller2_both_0_1_ac2_av_source', 'Branch ID'), ('miller2_both_0_1_ac2_av_target', 'PrBroCounID')): 0.7185185185185186, (('miller2_both_0_1_ac2_av_source', 'Fund centre name'), ('miller2_both_0_1_ac2_av_target', 'OrganCl')): 0.7143833943833944, (('miller2_both_0_1_ac2_av_source', 'Section name'), ('miller2_both_0_1_ac2_av_target', 'SeN')): 0.7128205128205127, (('miller2_both_0_1_ac2_av_source', 'Branch ID'), ('miller2_both_0_1_ac2_av_target', 'CouReID')): 0.711111111111111, (('miller2_both_0_1_ac2_av_source', 'Division ID'), ('miller2_both_0_1_ac2_av_target', 'OrganCl')): 0.7109502262443439, (('miller2_both_0_1_ac2_av_source', 'Division ID'), ('miller2_both_0_1_ac2_av_target', 'PrBroCounID')): 0.7086868686868688, (('miller2_both_0_1_ac2_av_source', 'CFLI (marker)'), ('miller2_both_0_1_ac2_av_target', 'ChiIs')): 0.7079365079365079}
#
#
# # print(d[tuple(inv_t)])
#
# import os
#
# print(os.path.sep)

# from zipfile import ZipFile
#
# # with ZipFile('output.zip', 'w') as zipObj:
# #     # Add multiple files to the zip
# #     zipObj.write('f1.txt')
# #     zipObj.write('f2.txt')
#
#
# import os
# # create a ZipFile object
# with ZipFile('output_dir.zip', 'w') as zipObj:
#     # Iterate over all the files in directory
#     for folderName, subfolders, filenames in os.walk('f'):
#         for filename in filenames:
#             # create complete filepath of file in directory
#             filePath = os.path.join(folderName, filename)
#             # Add file to zip
#             zipObj.write(filePath, filePath)


# import pandas as pd
#
# file_path = "minio-volume/fabricated-data/test/Joinable/miller_accidents_1_joinable_both_noisy_schemata/miller_accidents_1_joinable_both_noisy_schemata_source.csv"
# df = pd.read_csv(file_path, index_col=False)
# print(df)
from itertools import product

dataset_folders = {'test_1/Joinable': {'miller_accidents_1_joinable_both_noisy_schemata': ['test_1/Joinable/miller_accidents_1_joinable_both_noisy_schemata/miller_accidents_1_joinable_both_noisy_schemata_matches.json', 'test_1/Joinable/miller_accidents_1_joinable_both_noisy_schemata/miller_accidents_1_joinable_both_noisy_schemata_source.csv', 'test_1/Joinable/miller_accidents_1_joinable_both_noisy_schemata/miller_accidents_1_joinable_both_noisy_schemata_source_schema.json', 'test_1/Joinable/miller_accidents_1_joinable_both_noisy_schemata/miller_accidents_1_joinable_both_noisy_schemata_target.csv', 'test_1/Joinable/miller_accidents_1_joinable_both_noisy_schemata/miller_accidents_1_joinable_both_noisy_schemata_target_schema.json'], 'miller_accidents_1_joinable_both_verbatim_schemata': ['test_1/Joinable/miller_accidents_1_joinable_both_verbatim_schemata/miller_accidents_1_joinable_both_verbatim_schemata_matches.json', 'test_1/Joinable/miller_accidents_1_joinable_both_verbatim_schemata/miller_accidents_1_joinable_both_verbatim_schemata_source.csv', 'test_1/Joinable/miller_accidents_1_joinable_both_verbatim_schemata/miller_accidents_1_joinable_both_verbatim_schemata_source_schema.json', 'test_1/Joinable/miller_accidents_1_joinable_both_verbatim_schemata/miller_accidents_1_joinable_both_verbatim_schemata_target.csv', 'test_1/Joinable/miller_accidents_1_joinable_both_verbatim_schemata/miller_accidents_1_joinable_both_verbatim_schemata_target_schema.json']}}
algorithm_configs = [{'Coma': {'max_n': 0, 'strategy': 'COMA_OPT'}}]
# for combination in product(dataset_folders.values(), algorithm_configs):
#     print(combination)


for dataset_name in dataset_folders.keys():
    dataset_pairs = dataset_folders[dataset_name]
    print(dataset_name)
    for combination in product(dataset_pairs.items(), algorithm_configs):
        dataset, algorithm_config = combination
        dataset_name, dataset_paths = dataset
        print(algorithm_configs)
        print(dataset_name, dataset_paths)
