from itertools import product
from multiprocessing import get_context
from typing import Union

import Levenshtein as Lv

from ..base_matcher import BaseMatcher
from ..match import Match
from ...data_sources.base_db import BaseDB
from ...data_sources.base_table import BaseTable


class JaccardLevenMatcher(BaseMatcher):
    """
    Class containing the methods for implementing a simple baseline matcher that uses Jaccard Similarity between
    columns to assess their correspondence score, enhanced by Levenshtein Distance.

    Methods
    -------
    jaccard_leven(list1, list2, threshold, process_pool)

    """

    def __init__(self, threshold_leven: float = 0.8, process_num: int = 1):
        """
        Parameters
        ----------
        threshold_leven : float, optional
            The Levenshtein ratio between the two column entries (lower ratio, the entries are more different)
        process_num : int, optional
            Te number of processes to spawn
        """
        self.threshold_leven = float(threshold_leven)
        self.process_num = int(process_num)

    def get_matches(self, source_input: Union[BaseDB, BaseTable], target_input: Union[BaseDB, BaseTable]):
        source_id = source_input.db_belongs_uid if isinstance(source_input, BaseTable) \
            else source_input.unique_identifier
        target_id = target_input.db_belongs_uid if isinstance(target_input, BaseTable) \
            else target_input.unique_identifier
        matches = []
        if self.process_num == 1:
            for combination in self.get_column_combinations(source_input.get_tables().values(),
                                                            target_input.get_tables().values(), self.threshold_leven,
                                                            target_id, source_id):
                matches.append(process_jaccard_leven(combination))
        else:
            with get_context("spawn").Pool(self.process_num) as process_pool:
                matches = list(process_pool.map(process_jaccard_leven,
                                                self.get_column_combinations(source_input.get_tables().values(),
                                                                             target_input.get_tables().values(),
                                                                             self.threshold_leven,
                                                                             target_id, source_id)))
        matches = list(filter(lambda elem: elem['sim'] > 0.0, matches))  # Remove the pairs with zero similarity
        sorted_matches = list(sorted(matches, key=lambda item: item['sim'], reverse=True))
        return sorted_matches

    @staticmethod
    def get_column_combinations(source_tables, target_tables, threshold, target_id, source_id):
        for source_table, target_table in product(source_tables, target_tables):
            for source_column, target_column in product(source_table.get_columns(), target_table.get_columns()):
                yield source_column.data, target_column.data, threshold, target_id, \
                      target_table.name, target_table.unique_identifier, \
                      target_column.name, target_column.unique_identifier, \
                      source_table.name, source_table.unique_identifier, source_id, \
                      source_column.name, source_column.unique_identifier


def process_jaccard_leven(tup: tuple):

    source_data, target_data, threshold, target_id, target_table_name, target_table_unique_identifier, \
        target_column_name, target_column_unique_identifier, source_table_name, source_table_unique_identifier, \
        source_id, source_column_name, source_column_unique_identifier = tup

    if len(set(source_data)) < len(set(target_data)):
        set1 = set(source_data)
        set2 = set(target_data)
    else:
        set1 = set(target_data)
        set2 = set(source_data)

    combinations = get_set_combinations(set1, set2, threshold)

    intersection_cnt = 0
    for cmb in combinations:
        intersection_cnt = intersection_cnt + process_lv(cmb)

    union_cnt = len(set1) + len(set2) - intersection_cnt

    if union_cnt == 0:
        sim = 0.0
    else:
        sim = float(intersection_cnt) / union_cnt

    return Match(target_id,
                 target_table_name, target_table_unique_identifier,
                 target_column_name, target_column_unique_identifier,
                 source_id,
                 source_table_name, source_table_unique_identifier,
                 source_column_name, source_column_unique_identifier,
                 sim).to_dict


def get_set_combinations(set1: set, set2: set, threshold: float):
    """
    Function that creates combination between elements of set1 and set2

    Parameters
    ----------
    set1 : set
        The first set that its elements will be taken
    set2 : set
        The second set
    threshold : float
        The Levenshtein ratio

    Returns
    -------
    generator
        A generator that yields one element from the first set, the second set and the Levenshtein ratio
    """
    for s1 in set1:
        yield str(s1), set2, threshold


def process_lv(tup: tuple):
    """
    Function that check if there exist entry from the second set that has a greater Levenshtein ratio with the
    element from the first set than the given threshold

    Parameters
    ----------
    tup : tuple
        A tuple containing one element from the first set, the second set and the threshold of the Levenshtein ratio

    Returns
    -------
    int
        1 if there is such an element 0 if not
    """
    s1, set2, threshold = tup
    for s2 in set2:
        if Lv.ratio(s1, str(s2)) >= threshold:
            return 1
    return 0
