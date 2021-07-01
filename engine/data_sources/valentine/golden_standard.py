class GoldenStandardLoader:
    """
    A class used to represent the golden standard (ground truth) of a dataset

    Attributes
    ----------
    expected_matches: set
        The expected matches in a set of frozensets
    size: int
        The size of the golden standard

    Methods
    -------
    load_golden_standard(path)
        Function that loads the golden standard from a JSON file

    is_in_golden_standard(): bool
        Function that checks if a mapping is in the golden standard
    """

    def __init__(self, json_dict):
        """
        Parameters
        ----------
        json_dict : dict
            The dictionary containing the mappings
        """
        self.expected_matches = set()
        self.size = 0
        self.load_golden_standard(json_dict)

    def load_golden_standard(self, json_dict: dict):
        """
        Function that loads the golden standard from a JSON file

        Parameters
        ----------
        json_dict : dict
            The dictionary containing the mappings
        """
        mappings: list = json_dict["matches"]
        for mapping in mappings:
            self.expected_matches.add(
                frozenset(((mapping["source_table"], mapping["source_column"]),
                          (mapping["target_table"], mapping["target_column"]))))
        self.size = len(self.expected_matches)

    def is_in_golden_standard(self, mapping: set):
        """
        Function that checks if a mapping is in the golden standard

        Parameters
        ----------
        mapping : set
            The mapping that we want to check

        Returns
        -------
        bool
            True if the mapping is in the golden standard false if not
        """
        return mapping in self.expected_matches
