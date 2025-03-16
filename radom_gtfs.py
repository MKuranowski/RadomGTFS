import argparse
import re

import requests
from impuls import App, HTTPResource, PipelineOptions
from impuls.model import Date, FeedInfo
from impuls.multi_file import IntermediateFeed, IntermediateFeedProvider, MultiFile
from impuls.tasks import AddEntity, ExecuteSQL, LoadGTFS


class RadomIntermediateFileProvider(IntermediateFeedProvider[HTTPResource]):
    def needed(self) -> list[IntermediateFeed[HTTPResource]]:
        with requests.get("https://mzdik.pl/?id=145") as r:
            r.raise_for_status()
            return [
                IntermediateFeed(
                    resource=HTTPResource.get("https://mzdik.pl" + m[1]),
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
            intermediate_pipeline_tasks_factory=lambda feed: [
                LoadGTFS(feed.resource_name),
                ExecuteSQL(
                    task_name="FixAgency",
                    statement=(
                        "UPDATE agencies SET name = 'MZDiK Radom', url ='https://mzdik.pl/', "
                        "fare_url = 'https://mzdik.pl/index.php?id=157'"
                    ),
                ),
                ExecuteSQL(
                    task_name="DropFeedInfo",
                    statement="DELETE FROM feed_info",
                ),
                AddEntity(
                    task_name="AddFeedInfo",
                    entity=FeedInfo(
                        "Mikołaj Kuranowski",
                        "https://mkuran.pl/gtfs/",
                        "pl",
                        feed.version,
                    ),
                ),
                ExecuteSQL(
                    task_name="FixRouteColor",
                    statement=(
                        "UPDATE routes SET "
                        "color = CASE "
                        "  WHEN short_name LIKE 'N%' THEN '000000' "
                        "  ELSE 'E31E24' END, "
                        "text_color = 'FFFFFF'"
                    ),
                ),
                # TODO: Fix stop locations
            ],
            final_pipeline_tasks_factory=lambda _: [
                # TODO: Add fares
                # TODO: SaveGTFS
            ],
        )


if __name__ == "__main__":
    RadomGTFS().run()
