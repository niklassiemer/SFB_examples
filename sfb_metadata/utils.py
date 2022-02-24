import mendeleev


class Compound:
    _elements = {}
    debug = False

    def __init__(self, compound_dict=None, **kwargs):
        self._compound_dict = compound_dict or {}
        self._compound_dict.update(kwargs)
        if self.debug:
            print(f"DEBUG: dict={self._compound_dict}, kwargs={kwargs}")
        self._tot_mass = None
        for element in self._compound_dict:
            self._get_element(element)

    @property
    def included_elements(self):
        return list(self._compound_dict.keys())

    @classmethod
    def from_wt_percent(cls, compound_dict, **kwargs):
        _compound_dict = compound_dict or {}
        _compound_dict.update(kwargs)

        at_percent_dict = {}
        for element, wt_percent in _compound_dict.items():
            cls._get_element(element)
            at_percent_dict[element] = wt_percent / cls._elements[element].atomic_weight
        c = cls(at_percent_dict)
        return cls(c.at_percent_dict)

    @staticmethod
    def scale_at_percent(compound_dict, scale):
        return {at: scale * c for at, c in compound_dict.items()}

    def add_atoms(self, compound_dict, at_percent):
        if at_percent < 1:
            print("Warn: expected percent.")
        for element in compound_dict:
            self._get_element(element)
        scaled_old_at_percent = self.scale_at_percent(self.at_percent_dict, (100 - at_percent) / 100.)
        scaled_add_at_percent = self.scale_at_percent(compound_dict, at_percent / 100.)

        new_compound_dict = scaled_old_at_percent.copy()
        for at, c in scaled_add_at_percent.items():
            if at in new_compound_dict:
                new_compound_dict[at] += c
            else:
                new_compound_dict[at] = c

        factor = self._compound_dict[self.included_elements[0]] / new_compound_dict[self.included_elements[0]]

        self.__init__(self.scale_at_percent(new_compound_dict, factor))

    @classmethod
    def _get_element(cls, element_symbol: str = None):
        if element_symbol not in cls._elements:
            el = getattr(mendeleev, element_symbol)
            cls._elements[el.symbol] = el
        return cls._elements[element_symbol]

    def __call__(self, element_symbol):
        if element_symbol in self._compound_dict:
            return self._get_element(element_symbol)
        else:
            raise ValueError("No such element in compound")

    @property
    def total_mass(self):
        if self._tot_mass is None:
            tot_mass = 0
            for element, c in self._compound_dict.items():
                tot_mass += self._elements[element].atomic_weight * c
            self._tot_mass = tot_mass
        return self._tot_mass

    @property
    def number_of_atoms(self):
        return sum(self._compound_dict.values())

    @property
    def wt_percent_dict(self):
        return {element: self._elements[element].atomic_weight * c / self.total_mass
                for element, c in self._compound_dict.items()}

    @property
    def at_percent_dict(self):
        return {element: c / self.number_of_atoms
                for element, c in self._compound_dict.items()}

    def __repr__(self):
        return "".join([f"{key}{value} " for key, value in self._compound_dict.items()])
