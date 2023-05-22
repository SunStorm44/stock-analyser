from abc import ABC, abstractmethod


class DataTransformer(ABC):
    def __init__(self, raw_data=None):
        self._raw_data = raw_data
        self._processed_data = None

    @property
    def raw_data(self):
        return self._raw_data

    @property
    def processed_data(self):
        return self._processed_data

    @raw_data.setter
    def raw_data(self, value):
        self._raw_data = value

    @processed_data.setter
    def processed_data(self, value):
        raise AttributeError("Processed_data is a read-only attribute.")

    @abstractmethod
    def process_data(self, raw_data):
        pass
