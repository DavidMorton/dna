
from src import *
from container import Container
from dependency_injector.wiring import Provide

class Main():
    def __init__(self, dna_analyzer:DNAAnalyzer = Provide['DNAAnalyzer']):
        self._dna_analyzer = dna_analyzer

    def run_analysis(self, filename:str):
        self._dna_analyzer.analyze(filename)


container:Container = Container()
Container.wire(container)

m = Main()
m.run_analysis('.data/dna_samples/dan.23andme.txt')