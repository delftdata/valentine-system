import random
from .add_noise_data import update_values
from .dataset import Dataset


def split(source: Dataset, pk: int, common: float, rand: bool, approx: bool, prc: int, approx_prc: int):
    records_num, col_num = source.data.shape
    common = int(common * col_num)
    if rand:
        print('todo')
        exit(-1)
    else:
        pop = list(range(col_num))
        pop.remove(pk)
        if common == 0:
            common_columns = [pk]
            common = 1
        else:
            common_columns = [pk] + list(random.sample(pop, common-1))
        col_num -= common
        store = source.data.iloc[:, common_columns].copy()
        if approx:
            store2 = store.copy()
            store2 = update_values(store2, prc, approx_prc)
        temp = source.data.drop(common_columns, axis=1)

        half = int(col_num/2)

        if approx:
            target1_data = store2.join(temp.iloc[:, :half], lsuffix='pk')
            target2_data = store.join(temp.iloc[:, half:], lsuffix='pk')
        else:
            target1_data = store.join(temp.iloc[:, :half], lsuffix='pk')
            target2_data = store.join(temp.iloc[:, half:], lsuffix='pk')

        target1_schema = {}
        target2_schema = {}

        pop = [x for x in pop if x not in common_columns]
        for i in common_columns:
            target1_schema[i] = source.schema[i].copy()
            target2_schema[i] = source.schema[i].copy()
        for i in range(0, half):
            target1_schema[pop[i]] = source.schema[pop[i]].copy()
            target2_schema[pop[half+i]] = source.schema[pop[half+i]].copy()

        if col_num % 2 == 1:
            target2_schema[pop[col_num-1]] = source.schema[pop[col_num-1]].copy()

        return Dataset(target1_schema, target1_data), Dataset(target2_schema, target2_data), common_columns

