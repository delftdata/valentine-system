import pandas as pd
import numpy as np
import random
import string
from multiprocessing.pool import ThreadPool
import unidecode

letters = list(string.ascii_lowercase)

proximity = {'a': ['q', 'w', 'z', 's', 'x'],
             'b': ['f', 'g', 'h', 'v', 'n'],
             'c': ['s', 'x', 'd', 'f', 'v'],
             'd': ['w', 'e', 'r', 's', 'x', 'c', 'f', 'v'],
             'e': ['w', 'r', 's', 'd', 'f', '2', '3', '4'],
             'f': ['e', 'r', 't', 'd', 'c', 'b', 'g', 'v'],
             'g': ['r', 't', 'y', 'f', 'b', 'h', 'v', 'n'],
             'h': ['t', 'y', 'u', 'b', 'm', 'j', 'g', 'n'],
             'i': ['u', 'o', 'j', 'k', 'l', '7', '8', '9'],
             'j': ['y', 'u', 'i', 'm', 'h', 'k', 'n'],
             'k': ['u', 'i', 'o', 'm', 'j', 'l'],
             'l': ['i', 'o', 'p', 'k'],
             'm': ['j', 'h', 'k', 'n'],
             'n': ['b', 'm', 'j', 'g', 'h'],
             'o': ['i', 'p', 'k', 'l', '8', '9', '0'],
             'p': ['o', 'l', '9', '0'],
             'q': ['w', 'a', 's', '1', '2'],
             'r': ['e', 't', 'd', 'f', 'g', '3', '4', '5'],
             's': ['q', 'w', 'e', 'a', 'z', 'x', 'd', 'c'],
             't': ['r', 'y', 'f', 'g', 'h', '4', '5', '6'],
             'u': ['y', 'i', 'j', 'h', 'k', '6', '7', '8'],
             'v': ['d', 'c', 'f', 'b', 'g'],
             'w': ['q', 'e', 'a', 's', 'd', '1', '2', '3'],
             'x': ['a', 'z', 's', 'd', 'c'],
             'y': ['t', 'u', 'j', 'g', 'h', '5', '6', '7'],
             'z': ['a', 's', 'x'],
             '1': ['q', 'w', '2'],
             '2': ['q', 'w', 'e', '1', '3'],
             '3': ['w', 'e', 'r', '2', '4'],
             '4': ['e', 'r', 't', '3', '5'],
             '5': ['r', 't', 'y', '4', '6'],
             '6': ['t', 'y', 'u', '5', '7'],
             '7': ['y', 'u', 'i', '6', '8'],
             '8': ['u', 'i', 'o', '7', '9'],
             '9': ['i', 'o', 'p', '8', '0'],
             '0': ['o', 'p', '9']
}
def clean_word(word: str):
    getVals = list([val for val in word if val.isalnum()])
    result = "".join(getVals)
    return result

def proximity_typo(letter: str):
    return random.choice(proximity[letter])

def perturb_data(data: np.ndarray):
    tmp = data[~np.isnan(data)]
    tmp_size = tmp.size
    sign = random.sample([-1, 1],  1)[0]
    perturb = sign * random.randint(10, 50) / 100

    mu = np.mean(tmp)
    st_dev = np.std(tmp)

    mu = mu + mu * perturb
    st_dev = st_dev + st_dev * perturb

    tmp2 = np.random.normal(mu, st_dev, tmp.size)
    tmp2 = np.absolute(tmp2)

    if ((tmp % 1  == 0).all()):
        tmp2 = np.around(tmp2)
    to_return = data.copy()
    count = 0
    for i in range(len(to_return)):
        if pd.isnull(to_return[[i]]):
            continue
        else:
            to_return[i] = tmp2[count]
            count += 1

    return to_return

def sub_job(args: tuple):
    col = args[0]
    frame = args[1]
    approx_prc = args[2]
    typeCol = args[3]

    if typeCol == np.dtype('float64'):
        np_frame = np.array(frame)
        new_frame =  perturb_data(np_frame)
        return col, new_frame
    elif not typeCol == 'object':
        return col, frame
    else:
        for rec in range(len(frame)):

            frame[rec] = unidecode.unidecode(str(frame[rec]))


            if pd.isnull([frame[rec]]):
                continue
            elif frame[rec].replace('-', '').isnumeric():
                frame[rec] = frame[rec].replace('-',' ')
            else:
                try:
                    word = list(clean_word(frame[rec]))
                except Exception as e:
                    print(frame[rec])
                    print(col, word)
                    raise Exception(e)
                hml = int(len(word) * approx_prc / 100)
                ltc = random.sample(range(len(word)), hml)
                for l in ltc:
                    pick = proximity_typo(word[l].lower())
                    while word[l] == pick:
                        pick = random.sample(letters, 1)[0]

                    word[l] = pick
                word = "".join(word)
                frame[rec] = word
        return col, frame


def update_values(frame: pd.DataFrame, prc: int, approx_prc: int):
    records_num, col_num = frame.shape

    rec_to_update = int(records_num * prc / 100)

    pool = ThreadPool(processes=2)
    output = dict(pool.imap_unordered(sub_job, input_generator(frame, approx_prc)))
    srt = {b: i for i, b in enumerate(list(frame))}
    output = dict(sorted(output.items(), key=lambda t:srt[t[0]]))
    pool.close()
    pert_pd = pd.DataFrame.from_dict(output)
    return pert_pd

def input_generator(frame: pd.DataFrame, approx_prc: int):
    for col in list(frame):
        yield col, frame[col].tolist(), approx_prc, str(frame[col].dtype)