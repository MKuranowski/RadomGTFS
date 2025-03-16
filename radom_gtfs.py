import argparse
import re
from typing import cast

import requests
from impuls import App, HTTPResource, PipelineOptions, Task
from impuls.model import Date, FareAttribute, FeedInfo
from impuls.multi_file import IntermediateFeed, IntermediateFeedProvider, MultiFile
from impuls.tasks import AddEntity, ExecuteSQL, GenerateTripHeadsign, LoadGTFS, SaveGTFS


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
    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("-o", "--output", default="radom.zip", help="path to the output GTFS")

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
                    ExecuteSQL(
                        task_name="FixAgency",
                        statement=(
                            "UPDATE agencies SET agency_id = '1', name = 'MZDiK Radom', "
                            "url ='https://mzdik.pl/', "
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
                    ExecuteSQL(
                        task_name="DropTripHeadsign",
                        statement="UPDATE trips SET headsign = ''",
                    ),
                    GenerateTripHeadsign(),
                    ExecuteSQL(
                        task_name="MarkRequestStops",
                        statement=(
                            "WITH request_stops AS "
                            "  (SELECT stop_id FROM stops WHERE name LIKE '%(NŻ)') "
                            "UPDATE stop_times SET "
                            "  pickup_type = iif(pickup_type = 0, 3, pickup_type), "
                            "  drop_off_type = iif(drop_off_type = 0, 3, drop_off_type) "
                            "WHERE stop_id IN request_stops"
                        ),
                    ),
                    # TODO: Fix stop locations
                ],
            ),
            final_pipeline_tasks_factory=lambda _: cast(
                list[Task],
                [
                    AddEntity(
                        task_name="AddSingleJourneyFare",
                        entity=FareAttribute(
                            agency_id="1",
                            id="single",
                            price=3.60,
                            currency_type="PLN",
                            payment_method=FareAttribute.PaymentMethod.ON_BOARD,
                            transfers=0,
                            transfer_duration=None,
                        ),
                    ),
                    AddEntity(
                        task_name="AddOneHourFare",
                        entity=FareAttribute(
                            agency_id="1",
                            id="one_hour",
                            price=5.00,
                            currency_type="PLN",
                            payment_method=FareAttribute.PaymentMethod.ON_BOARD,
                            transfers=None,
                            transfer_duration=3600,
                        ),
                    ),
                    AddEntity(
                        task_name="AddOneDayFare",
                        entity=FareAttribute(
                            agency_id="1",
                            id="one_day",
                            price=14.00,
                            currency_type="PLN",
                            payment_method=FareAttribute.PaymentMethod.ON_BOARD,
                            transfers=None,
                            transfer_duration=86400,
                        ),
                    ),
                    SaveGTFS(
                        headers={
                            "agency.txt": (
                                "agency_id",
                                "agency_name",
                                "agency_url",
                                "agency_timezone",
                                "agency_lang",
                                "agency_fare_url",
                            ),
                            "calendar_dates.txt": ("service_id", "date", "exception_type"),
                            "stops.txt": ("stop_id", "stop_name", "stop_lat", "stop_lon"),
                            "routes.txt": (
                                "route_id",
                                "agency_id",
                                "route_short_name",
                                "route_long_name",
                                "route_type",
                                "route_color",
                                "route_text_color",
                            ),
                            "trips.txt": (
                                "trip_id",
                                "route_id",
                                "service_id",
                                "trip_headsign",
                                "direction_id",
                                "shape_id",
                            ),
                            "stop_times.txt": (
                                "trip_id",
                                "stop_sequence",
                                "stop_id",
                                "arrival_time",
                                "departure_time",
                                "pickup_type",
                                "drop_off_type",
                            ),
                            "shapes.txt": (
                                "shape_id",
                                "shape_pt_sequence",
                                "shape_pt_lat",
                                "shape_pt_lon",
                            ),
                            "fare_attributes.txt": (
                                "agency_id",
                                "fare_id",
                                "price",
                                "currency_type",
                                "payment_method",
                                "transfers",
                                "transfer_duration",
                            ),
                            "feed_info.txt": (
                                "feed_publisher_name",
                                "feed_publisher_url",
                                "feed_lang",
                                "feed_version",
                            ),
                        },
                        target=args.output,
                    ),
                ],
            ),
        )


if __name__ == "__main__":
    RadomGTFS().run()
