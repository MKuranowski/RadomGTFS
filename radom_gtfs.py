import argparse
import re
from typing import cast

import requests
from impuls import App, HTTPResource, PipelineOptions, Task
from impuls.model import Date
from impuls.multi_file import IntermediateFeed, IntermediateFeedProvider, MultiFile
from impuls.tasks import LoadGTFS


class RadomIntermediateFileProvider(IntermediateFeedProvider[HTTPResource]):
    def needed(self) -> list[IntermediateFeed[HTTPResource]]:
        with requests.get("http://mzdik.radom.pl/?id=145") as r:
            r.raise_for_status()
            return [
                IntermediateFeed(
                    resource=HTTPResource.get("http://mzdik.radom.pl" + m[1]),
                    resource_name=f"Rozklady-{m[2]}-{m[3]}-{m[4]}{m[5]}.zip",
                    version=f"{m[2]}-{m[3]}-{m[4]}{m[5]}",
                    start_date=Date(int(m[2]), int(m[3]), int(m[4])),
                )
                for m in re.finditer(
                    r"href=\"(/upload/file/Rozklady-([0-9]{4})-([0-9]{2})-([0-9]{2})(\w?)\.zip)\"",
                    r.text,
                )
            ]


class RadomGTFS(App):
    def prepare(
        self,
        args: argparse.Namespace,
        options: PipelineOptions,
    ) -> MultiFile[HTTPResource]:
        return MultiFile(
            options=options,
            intermediate_provider=RadomIntermediateFileProvider(),
            intermediate_pipeline_tasks_factory=lambda feed: cast(
                list[Task],
                [
                    LoadGTFS(feed.resource_name),
                    # TODO: Prettify agency
                    # TODO: Fix feed_info.txt
                    # TODO: Prettify routes
                    # TODO: Fix trip headsigns
                    # TODO: Fix stop locations
                ],
            ),
            final_pipeline_tasks_factory=lambda _: [
                # TODO: Add fares
                # TODO: SaveGTFS
            ],
        )


if __name__ == "__main__":
    RadomGTFS().run()
