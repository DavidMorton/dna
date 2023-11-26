from src import *
from container import Container
from dependency_injector.wiring import Provide

raise Exception('Read the readme, then comment this out if you still want to run this code.')

class Main():
    def __init__(self, dna_analyzer:DNAAnalyzer = Provide['DNAAnalyzer']):
        self._dna_analyzer = dna_analyzer

    def run_analysis(self, filename:str):
        self._dna_analyzer.analyze(filename)

if __name__ == '__main__':
    container:Container = Container()
    Container.wire(container)

    m = Main()
    m.run_analysis('.data/dna_samples/david.23andme.txt')
