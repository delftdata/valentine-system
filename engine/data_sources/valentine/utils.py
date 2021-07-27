import math


class BenchmarkFiles:

    def __init__(self, paths: list[str]):
        for file_path in paths:
            if file_path.endswith('matches.json'):
                self.golden_standard_path: str = file_path
            elif file_path.endswith('source_schema.json'):
                self.source_schema: str = file_path
            elif file_path.endswith('source.csv'):
                self.source_data: str = file_path
            elif file_path.endswith('target_schema.json'):
                self.target_schema: str = file_path
            elif file_path.endswith('target.csv'):
                self.target_data: str = file_path


def one_to_one_matches(matches: dict):
    """
    A filter that takes a dict of column matches and returns a dict of 1 to 1 matches. The filter works in the following
    way: At first it gets the median similarity of the set of the values and removes all matches
    that have a similarity lower than that. Then from what remained it matches columns for me highest similarity
    to the lowest till the columns have at most one match.

    Parameters
    ----------
    matches : dict
        The ranked list of matches

    Returns
    -------
    dict
        The ranked list of matches after the 1 to 1 filter
    """
    set_match_values = set(matches.values())

    if len(set_match_values) < 2:
        return matches

    matched = dict()

    for key in matches.keys():
        matched[key[0]] = False
        matched[key[1]] = False

    median = list(set_match_values)[math.ceil(len(set_match_values)/2)]

    matches1to1 = dict()

    for key in matches.keys():
        if (not matched[key[0]]) and (not matched[key[1]]):
            similarity = matches.get(key)
            if similarity >= median:
                matches1to1[key] = similarity
                matched[key[0]] = True
                matched[key[1]] = True
            else:
                break
    return matches1to1
