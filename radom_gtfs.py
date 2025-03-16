import argparse

from impuls import App, HTTPResource, PipelineOptions
from impuls.multi_file import IntermediateFeed, IntermediateFeedProvider, MultiFile


class RadomIntermediateFileProvider(IntermediateFeedProvider[HTTPResource]):
    def needed(self) -> list[IntermediateFeed[HTTPResource]]:
        raise NotImplementedError("TODO")


class RadomGTFS(App):
    def prepare(
        self,
        args: argparse.Namespace,
        options: PipelineOptions,
    ) -> MultiFile[HTTPResource]:
        return MultiFile(
            options=options,
            intermediate_provider=RadomIntermediateFileProvider(),
            intermediate_pipeline_tasks_factory=lambda _: [],
            final_pipeline_tasks_factory=lambda _: [],
        )


if __name__ == "__main__":
    RadomGTFS().run()
