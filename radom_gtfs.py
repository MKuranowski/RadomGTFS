import argparse
import csv
import io
import re
from collections.abc import Iterable, Iterator
from typing import cast
from xml.etree import ElementTree

import requests
from impuls import App, PipelineOptions, Task
from impuls.model import Date, FareAttribute, FeedInfo
from impuls.multi_file import IntermediateFeed, IntermediateFeedProvider, MultiFile
from impuls.resource import FETCH_CHUNK_SIZE, HTTPResource, WrappedResource
from impuls.tasks import (
    AddEntity,
    ExecuteSQL,
    GenerateTripHeadsign,
    LoadGTFS,
    ModifyStopsFromCSV,
    SaveGTFS,
)


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


class RadomStopsResource(WrappedResource):
    def __init__(self) -> None:
        super().__init__(
            HTTPResource.post(
                "http://rkm.mzdik.radom.pl/PublicService.asmx",
                headers={"Content-Type": "text/xml; charset=utf-8"},
                data=(  # type: ignore  # https://github.com/MKuranowski/Impuls/issues/23
                    "<?xml version='1.0' encoding='utf-8'?>"
                    "<soap:Envelope xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' "
                    "               xmlns:xsd='http://www.w3.org/2001/XMLSchema' "
                    "               xmlns:soap='http://schemas.xmlsoap.org/soap/envelope/'>"
                    "  <soap:Body><GetGoogleStops xmlns='http://PublicService/' /></soap:Body>"
                    "</soap:Envelope>"
                ),
            ),
        )

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        xml_content = self._get_xml_content(conditional)
        root = ElementTree.XML(xml_content)
        stops = self.extract_stops_from_xml(root)
        csv_content = self.dump_stops_to_csv(stops)
        for offset in range(0, len(csv_content), FETCH_CHUNK_SIZE):
            yield csv_content[offset : offset + FETCH_CHUNK_SIZE]

    def _get_xml_content(self, conditional: bool) -> str:
        return b"".join(self.r.fetch(conditional)).decode("utf-8-sig")

    def extract_stops_from_xml(self, xml: ElementTree.Element) -> list[tuple[str, ...]]:
        return [
            (
                s.get("id", "").strip(),
                self.prettify_name(s.get("n", "").strip()),
                s.get("y", "").strip(),
                s.get("x", "").strip(),
            )
            for s in xml.iterfind(".//S")
        ]

    @staticmethod
    def dump_stops_to_csv(stops: Iterable[tuple[str, ...]]) -> bytes:
        buffer = io.BytesIO()
        text_buffer = io.TextIOWrapper(buffer, encoding="utf-8", newline="")
        writer = csv.writer(text_buffer)
        writer.writerow(("stop_id", "stop_name", "stop_lat", "stop_lon"))
        writer.writerows(stops)
        text_buffer.flush()
        return buffer.getvalue()

    @staticmethod
    def prettify_name(name: str) -> str:
        return name.rstrip(" .").replace("  ", " ")


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
            additional_resources={"stops.csv": RadomStopsResource()},
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
                    ModifyStopsFromCSV("stops.csv"),
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
