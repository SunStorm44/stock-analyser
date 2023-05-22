from abc import ABC, abstractmethod

from src.config import CREDENTIALS


class DataExtractor(ABC):
    def __init__(self):
        self.credentials = CREDENTIALS

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def get_data(self):
        pass
