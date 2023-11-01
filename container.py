from dependency_injector import containers, providers
from src import *

class Container(containers.DeclarativeContainer):
    container = providers.Object(None)
    DNAAnalyzer = providers.Singleton(DNAAnalyzer)
    Options = providers.Singleton(Options)
    CitationsDataframeGenerator = providers.Singleton(CitationsDataframeGenerator)
    NCBIDataDownloader = providers.Singleton(NCBIDataDownloader)
    NCBIDataFrameGenerator = providers.Singleton(NCBIDataFrameGenerator)
    AlleleTableGenerator = providers.Singleton(AlleleTableGenerator)
    AncestryReader = providers.Singleton(AncestryReader)
    TwentyThreeReader = providers.Singleton(TwentyThreeReader)
    GeneticFileReader = providers.Singleton(GeneticFileReader)
    GeneticDataToDataFrameConverter = providers.Singleton(GeneticDataToDataFrameConverter)
    OpenSNPAggregator = providers.Singleton(OpenSNPAggregator)
    OpenSNPDownloader = providers.Singleton(OpenSNPDownloader)
    OpenSNPProgress = providers.Singleton(OpenSNPProgress)

    def wire(container: containers.DeclarativeContainer, mod='__main__'):
        import src
        packages = [src]

        import __main__

        container.wire(
            modules=[mod],
            packages=packages
        )
        container.container.override(providers.Object(container))