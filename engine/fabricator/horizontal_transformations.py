from .dataset import Dataset
from .add_noise_data import update_values
import random
import copy


def split(source: Dataset, overlap: float, rand: bool, approx: bool, prc: int, approx_prc: int):
    records_num, col_num = source.data.shape
    half = int(records_num/2)
    target1_data = source.data.iloc[:half]
    target2_data = source.data.iloc[half:]

    if rand:
        tmp = target2_data.iloc[random.sample(range(half), int(overlap * half))]
        if approx:
            tmp = update_values(tmp, prc, approx_prc)
        target1_data = target1_data.append(tmp)
    else:
        tmp = target2_data.iloc[:int(overlap * half)]
        if approx:
            tmp = update_values(tmp, prc, approx_prc)
        target1_data = target1_data.append(tmp)

    common_columns = source.schema.keys()

    return Dataset(copy.deepcopy(source.schema), target1_data), Dataset(copy.deepcopy(source.schema), target2_data), common_columns


def overlap2sources(source1: Dataset, source2: Dataset, overlap: float, rand: bool):
    records_num, col_num = source1.data.shape
    half = int(records_num / 2)
    target1_data = source1.data.iloc[:half]
    target2_data = source2.data.iloc[half:]

    if rand:
        commons = random.sample(range(half), int(overlap * half))
        target2_data = target2_data.append(source2.data.iloc[commons])

    else:
        target2_data = target2_data.append(source2.data.iloc[:int(overlap * half)])

    return Dataset(source1.schema, target1_data), Dataset(source2.schema, target2_data)
