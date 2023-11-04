from .ancestry_reader import AncestryReader
from .twentythree_reader import TwentyThreeReader
from .genetic_file_reader import GeneticFileReader
from dependency_injector.wiring import Provide

class GeneticDataToDataFrameConverter:
    def __init__(self,
                 ancestry_reader:AncestryReader = Provide['AncestryReader'],
                 twenty_three_reader:TwentyThreeReader = Provide['TwentyThreeReader']):
        self._readers = {
            'ancestry.txt': ancestry_reader,
            '23andme.txt': twenty_three_reader
        }

    def _get_reader(self, filename)-> GeneticFileReader:
        readers = [v for k,v in self._readers.items() if filename.endswith(k)]
        if len(readers) == 1:
            return readers[0]
        return None

    def read_data(self, filename, throw_on_error=False):
        reader = self._get_reader(filename)
        if reader is not None:
            return reader.get_file_data(filename, throw_on_error)
        return None
