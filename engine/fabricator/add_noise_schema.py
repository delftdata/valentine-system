import re
from .dataset import Dataset
import random
import math


def abbreviate(name: str):
    abbreviation = []
    name = ''.join([w[0].upper()+w[1:] for w in name.split(' ')])
    words = name.split('_')
    if len(words) != 1:
        for w in words:
            if len(w) >= 1:
                bound = random.randint(math.ceil(len(w)/4), math.ceil(len(w)/2))
            else:
                bound = 1
            abbreviation += w[:bound]
        abbreviation = ''.join(abbreviation)
        abbreviation = abbreviation.upper()

    else:
        capitals = re.findall('^[a-z]+|[A-Z][a-z]*|\d+', name)
        if len(capitals) >= 1:
            for c in capitals:
                if len(c) > 1:
                    bound = random.randint(math.ceil(len(c)/4), math.ceil(len(c)/2))
                else:
                    bound = 1
                abbreviation += [c[:bound]]
            abbreviation = ''.join(abbreviation)

    return abbreviation


def augment(name: str, table_name: str):
    if len(name.split(' ')) > 1:
        name = ''.join([w[0].upper() + w[1:] if len(w) > 1 else w[0].upper() for w in name.split(' ')])
    return table_name.split('_')[0]+'_'+name


def drop_vowels(word: str):
    word = ''.join([w[0].upper() + w[1:] if len(w) > 1 else w[0].upper() for w in word.split(' ')])
    table = str.maketrans(dict.fromkeys('aeiouyAEIOUY'))
    return word.translate(table).lower()


def approximate_column_names(dataset: Dataset, table_name: str, choice: int):
    schema = dataset.schema
    columns = []

    for i in schema.keys():
        columns.append(schema[i][0])

    new_columns = []

    if choice == 1:
        for col in columns:
            col = augment(col, table_name)
            new_columns.append(col)

    elif choice == 2:
        for col in columns:
            newcol = ''
            if len(col.split('_')) >= 1 or len(re.findall('[A-Z][^A-Z]*', col)) >= 1:
                newcol = abbreviate(col)
            if newcol in new_columns:
                new_columns.append(col)
            else:
                new_columns.append(newcol)

    elif choice == 3:
        for col in columns:
            newcol = drop_vowels(col)
            if newcol in new_columns:
                new_columns.append(col)
            else:
                new_columns.append(newcol)

    elif choice == 4:
        for col in columns:
            newcol = ''
            if len(col.split('_')) >= 1 or len(re.findall('[A-Z][^A-Z]*', col)) >= 1:
                newcol = abbreviate(col)
            newcol = augment(newcol, table_name)
            if newcol in new_columns:
                new_columns.append(col)
            else:
                new_columns.append(newcol)

    elif choice == 5:
        for col in columns:
            newcol = drop_vowels(col)
            newcol = augment(newcol, table_name)
            if newcol in new_columns:
                new_columns.append(col)
            else:
                new_columns.append(newcol)

    count = 0
    for i in schema.keys():
        schema[i][0] = new_columns[count]
        count += 1

    return Dataset(schema, dataset.data)
