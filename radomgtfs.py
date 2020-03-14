from urllib.parse import urljoin
from email.utils import parsedate_to_datetime
from datetime import datetime, date, timedelta
from warnings import warn
from tzlocal import get_localzone
from typing import Tuple, List, Mapping, AbstractSet, Optional
from bs4 import BeautifulSoup
import subprocess
import requests
import argparse
import tempfile
import zipfile
import shutil
import zeep
import time
import csv
import os
import io
import re

# CONSTANTS #

__title__ = "RadomGTFS"
__author__ = "Mikolaj Kuranowski"
__email__ = "mikolaj@mkuran.pl"
__license__ = "MIT"

KNOWN_SERVICES = {"POWSZEDNI", "SOBOTA", "NIEDZIELA"}
IGNORE_STOPS = {1220, 1221, 1222, 1223, 1224, 1225, 1226, 1227, 1228, 1229,
                649, 652, 653, 659, 662}

# HELPER FUNCTIONS #

def dump_mdb_table(mdb_file: str, table_name: str) -> Tuple[io.StringIO, csv.DictReader]:
    result = subprocess.run(
        ["mdb-export", mdb_file, table_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={"MDB_JET3_CHARSET": "CP1250"},
        encoding="utf-8",
        check=True
    )

    buffer = io.StringIO(result.stdout, newline="")
    reader = csv.DictReader(buffer)
    return buffer, reader

def clear_directory(directory):
    if os.path.exists(directory) and os.path.isdir(directory):
        for f in os.scandir(directory):
            if f.is_dir():
                shutil.rmtree(f.path)
            else:
                os.remove(f.path)
    else:
        os.mkdir(directory)

def route_name(short_name: str) -> str:
    short_name = short_name.rjust(4, "0")

    website = requests.get(f"http://www.mzdik.pl/rozklady/{short_name.lower()}/w.htm")
    website.encoding = "latin2"
    soup = BeautifulSoup(website.text, "html.parser")

    dirs = list(map(lambda i: i.find("b").string, soup.find_all("td", colspan="3")))

    # We only need 2 direction names to create route_long_name
    del dirs[2:]

    # Only 1 direction name means this is a loop route
    if len(dirs) == 1:
        dirs.append(dirs[0])

    return " — ".join(dirs)

def gtfs_time(minutes_after_midnight: int) -> str:
    return "{:0>2}:{:0>2}:00".format(*divmod(minutes_after_midnight, 60))

def calendar_exceptions() -> Mapping[date, str]:
    print("\033[1A\033[K" "Downloading calendar exceptions")

    req = requests.get("https://docs.google.com/spreadsheets/d/"
                       "1kSCBQyIE8bz2NgqpzyS75I7ndnlp4dhD3TmEY2jO7K0/export?format=csv")
    req.encoding = "utf8"
    req.raise_for_status()

    buff = io.StringIO(req.text, newline="")
    reader = csv.DictReader(buff)
    exceptions = {}

    for row in reader:
        row["date"] = datetime.strptime(row["date"], "%Y-%m-%d").date()

        if row["regions"] != "" and not ("14" in row["regions"].split(".")):
            continue

        exceptions[row["date"]] = row["exception"]

    buff.close()
    return exceptions

def compress(target):
    with zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED) as arch:
        for f in os.scandir("gtfs"):
            if f.name.endswith(".txt"):
                arch.write(f.path, arcname=f.name)

# DATA DOWNLOADING #

def list_files() -> List[dict]:
    website = requests.get("http://www.mzdik.radom.pl/index.php?id=145")
    soup = BeautifulSoup(website.text, "html.parser")
    anchors = soup.find_all("a", href=re.compile(r"/upload/file/Rozklady.+\.zip"))
    files = []

    if len(anchors) == 0:
        raise RuntimeError("Schedules file not found on http://mzdik.radom.pl/?id=145")

    for anchor in anchors:
        href = anchor.get("href")
        link = urljoin("http://www.mzdik.radom.pl/index.php?id=145", href)
        version = re.search(r"[0-9-]+", href)

        if not version:
            raise ValueError(f"unable to get feed_version from href {href!r}")
        else:
            version = version[0].lstrip("-")

        start_date = datetime.strptime(version, "%Y-%m-%d").date()

        files.append({
            "url": link,
            "path": os.path.join("db", version + ".mdb"),
            "version": version,
            "start": start_date,
            "end": start_date + timedelta(days=180),
        })

    # Add proper end dates
    for idx in range(len(files) - 1):
        files[idx]["end"] = files[idx + 1]["start"] - timedelta(days=1)

    # Remove files ending in the past
    today = date.today()
    files = [i for i in files if i["end"] >= today]

    return files

def unpack_zip(req, target, version):
    zip_stream = io.BytesIO(req.content)

    with zipfile.ZipFile(zip_stream) as arch:

        zip_files = arch.namelist()
        if len(zip_files) == 1:
            dbase_name = zip_files[0]
        else:
            raise ValueError(f"zipfile corresponding to version {version} "
                             f"has more then one file inside: {zip_files}")

        arch.extract(dbase_name, path="db")
        os.rename(os.path.join("db", dbase_name), target)

def get_files(files) -> bool:
    os.makedirs("db", exist_ok=True)
    things_changed = False

    def is_in_files(version):
        for i in files:
            if i["version"] == version:
                return True
        return False

    # Remove unwanted files
    for f in os.scandir("db"):
        f_ver = f.name.rstrip(".mdb")

        if not is_in_files(f_ver):
            os.remove(f.path)
            things_changed = True

    # Download missing databases
    for file_info in files:

        try:
            local_modtime = os.stat(file_info["path"]).st_mtime
        except FileNotFoundError:
            local_modtime = 0

        # Turn the timestamp into an aware datetime object
        local_modtime = datetime.fromtimestamp(local_modtime, tz=get_localzone())

        # Make a request for the file
        req = requests.get(file_info["url"], stream=True)
        req.raise_for_status()

        remote_modtime = parsedate_to_datetime(req.headers["Last-Modified"])

        # The uploaded file is newer, redownload it
        if remote_modtime > local_modtime:
            things_changed = True
            unpack_zip(req, file_info["path"], file_info["version"])

        req.close()

    return things_changed

# DATA PARSING #

class StopHandler:
    def __init__(self):
        self.used_missing = set()  # type: AbstractSet[int]
        self.invalid = {}  # type: Mapping[int, str]
        self.used = set()  # type: AbstractSet[int]
        self.data = {}     # type: Mapping[int, Mapping[str, str]]

    def read_data_csv(self):
        """Get stop positions from file stops.csv"""
        # Download stops
        file = open("stops.csv", "r", encoding="utf-8")
        reader = csv.DictReader(file)

        # Iterate over stops
        for stop in reader:
            stop_id = int(stop.get("id"))

            if stop_id in IGNORE_STOPS:
                continue

            self.data[stop_id] = {
                "stop_id": stop_id,
                "stop_name": stop.get("nazwa"),
                "stop_lat": stop.get("szerokosc"),
                "stop_lon": stop.get("dlugosc"),
            }

    def read_data_mybus(self):
        """Get stop positions from http://rkm.mzdik.radom.pl/"""
        client = zeep.Client("http://rkm.mzdik.radom.pl/PublicService.asmx?WSDL")
        service = client.create_service("{http://PublicService/}PublicServiceSoap",
                                        "http://rkm.mzdik.radom.pl/PublicService.asmx")

        stops = service.GetGoogleStops()
        stops = stops.findall("S")

        if len(stops) == 0:
            raise RuntimeError("no stops returned from rkm.mzdik.radom.pl")

        for stop in stops:

            stop_id = int(stop.get("id"))

            if stop_id in IGNORE_STOPS:
                continue

            self.data[stop_id] = {
                "stop_id": stop_id,
                "stop_name": stop.get("n").strip(),
                "stop_lat": stop.get("y"),
                "stop_lon": stop.get("x"),
            }

    def read_table(self, mdb_file):
        # Open database dump
        buff, reader = dump_mdb_table(mdb_file, "tStakes")

        # Load stops
        for stop in reader:
            stop_id = int(stop["ID"])

            # Invalid Stop
            if stop_id not in self.data:
                self.invalid[stop_id] = stop["nSymbol"], stop["nName"]

        buff.close()

    def check_id(self, stop_id: int) -> bool:
        """Check if this stop_id can be outputed to stop_times.txt"""

        if stop_id in IGNORE_STOPS:
            return False

        elif stop_id in self.data:
            return True

        else:
            self.used_missing.add(stop_id)
            return False

    def use_id(self, stop_id: int):
        """Mark this stop_id as used"""
        self.used.add(stop_id)

    def export(self):
        """Save all used stops to stops.txt and
        all used stops without location to missing_stops.csv"""

        # Known stops
        with open("gtfs/stops.txt", mode="w", encoding="utf-8", newline="") as f:
            header = ["stop_id", "stop_name", "stop_lat", "stop_lon"]
            writer = csv.DictWriter(f, header)
            writer.writeheader()

            for used_stop_id in sorted(self.used):
                writer.writerow(self.data[used_stop_id])

        # Unknown stops
        with open("missing_stops.csv", mode="w", encoding="utf-8", newline="") as f:
            wrtr = csv.writer(f)
            wrtr.writerow(["id", "code", "name"])

            for used_invalid_stop in self.used_missing:
                stop_code, stop_name = self.invalid.get(used_invalid_stop, ("", ""))
                wrtr.writerow([used_invalid_stop, stop_code, stop_name])

class RadomGtfs:
    def __init__(self):
        # normal attributes
        self.stops = StopHandler()
        self.routes_used = set()
        self.cal_exceptions = calendar_exceptions()

        # attributes changing per input file
        self.start_date = date.today()
        self.end_date = self.start_date + timedelta(days=180)

        self.services_used = set()

        self.id_prefix = ""
        self.mdb_file = ""

        self.daytype_to_service = {}
        self.pattern_to_route = {}
        self.trips = {}

        # files that are written to multiple times
        self.trips_head = ["route_id", "service_id", "trip_id", "trip_headsign"]
        self.times_head = ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"]
        self.dates_head = ["service_id", "date", "exception_type"]

        self.trips_buff = None
        self.trips_wrtr = None

        self.times_buff = None
        self.times_wrtr = None

        self.dates_buff = None
        self.dates_wrtr = None

    def open_files(self):
        self.trips_buff = open("gtfs/trips.txt", mode="w", encoding="utf-8", newline="")
        self.trips_wrtr = csv.DictWriter(self.trips_buff, self.trips_head)
        self.trips_wrtr.writeheader()

        self.times_buff = open("gtfs/stop_times.txt", mode="w", encoding="utf-8", newline="")
        self.times_wrtr = csv.DictWriter(self.times_buff, self.times_head)
        self.times_wrtr.writeheader()

        self.dates_buff = open("gtfs/calendar_dates.txt", mode="w", encoding="utf-8", newline="")
        self.dates_wrtr = csv.writer(self.dates_buff)
        self.dates_wrtr.writerow(self.dates_head)

    def close(self):
        self.trips_buff.close()
        self.times_buff.close()
        self.dates_buff.close()

    # Functions executed once

    @staticmethod
    def static_files(feed_version, data_update, fp_name=None, fp_url=None):
        # agency.txt
        with open("gtfs/agency.txt", "w", encoding="utf-8", newline="\r\n") as f:
            f.write("agency_id,agency_name,agency_url,agency_timezone,agency_lang\n")
            f.write('0,MZDiK Radom,"http://www.mzdik.radom.pl/",Europe/Warsaw,pl\n')

        # attribution.txt
        with open("gtfs/attributions.txt", "w", encoding="utf-8", newline="\r\n") as f:
            f.write("attribution_id,organization_name,is_producer,is_operator,"
                    "is_authority,is_data_source,attribution_url\n")
            f.write('0,"RadomGTFS (provided by Mikołaj Kuranowski)",1,0,0,0,'
                    '"https://github.com/MKuranowski/RadomGTFS"\n')
            f.write(f'1,"MZDiK Radom (data retrieved {data_update})",0,0,1,1,'
                    '"http://www.mzdik.radom.pl/"\n')

        # feed_info.txt, but only if feed_publisher data is provided
        if fp_name and fp_url:
            with open("gtfs/feed_info.txt", "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["feed_publisher_name", "feed_publisher_url",
                                "feed_lang", "feed_version"])
                writer.writerow([fp_name, fp_url, "pl", feed_version])

    def routes(self):
        with open("gtfs/routes.txt", mode="w", encoding="utf-8", newline="") as f:
            wrtr = csv.writer(f)
            wrtr.writerow(["agency_id", "route_id", "route_short_name", "route_long_name",
                           "route_type", "route_color", "route_text_color"])

            for route_id in sorted(self.routes_used, key=lambda i: i.rjust(4, "0")):
                long_name = route_name(route_id)
                wrtr.writerow(["0", route_id, route_id, long_name, "3", "E31E24", "FFFFFF"])

    def verify_services(self):
        valid_services = set()
        remove_trips = set()

        # Read used services
        with open("gtfs/calendar_dates.txt", mode="r", encoding="utf-8", newline="") as f:
            valid_services = {i["service_id"] for i in csv.DictReader(f)}

        # Verify trips
        os.rename("gtfs/trips.txt", "gtfs/trips.txt.old")
        with open("gtfs/trips.txt.old", mode="r", encoding="utf-8", newline="") as in_f, \
                open("gtfs/trips.txt", mode="w", encoding="utf-8", newline="") as out_f:

            # Create csv writers and readers
            in_r = csv.DictReader(in_f)

            out_w = csv.DictWriter(out_f, self.trips_head)
            out_w.writeheader()

            for row in in_r:
                # If trip is in valid_services just re-write it
                if row["service_id"] in valid_services:
                    out_w.writerow(row)

                # Otherwise mark trip_id to be removed
                else:
                    remove_trips.add(row["trip_id"])

        os.remove("gtfs/trips.txt.old")

        # Verify stop_times
        os.rename("gtfs/stop_times.txt", "gtfs/stop_times.txt.old")
        with open("gtfs/stop_times.txt.old", mode="r", encoding="utf-8", newline="") as in_f, \
                open("gtfs/stop_times.txt", mode="w", encoding="utf-8", newline="") as out_f:

            # Create csv writers and readers
            in_r = csv.DictReader(in_f)

            out_w = csv.DictWriter(out_f, self.times_head)
            out_w.writeheader()

            for row in in_r:
                # If trip is marked to be removed, move to next row
                if row["trip_id"] in remove_trips:
                    pass

                # Otherwise re-write row
                else:
                    out_w.writerow(row)

        os.remove("gtfs/stop_times.txt.old")

    # Functions executed for each database

    def new_file(
            self,
            file_name: str,
            id_prefix: str = "",
            start_date: Optional[date] = None,
            end_date: Optional[date] = None):

        self.start_date = start_date or date.today()
        self.end_date = self.start_date + timedelta(days=180)

        self.services_used = set()

        self.id_prefix = id_prefix
        self.mdb_file = file_name

        self.daytype_to_service = {}
        self.pattern_to_route = {}
        self.trips = {}

    def map_patterns(self):
        # Map database route IDs to GTFS route_id.
        buff, reader = dump_mdb_table(self.mdb_file, "tLines")
        convert_route_id = {i["ID"]: i["nNumber"] for i in reader}
        buff.close()

        # Map nDir → route_id
        buff, reader = dump_mdb_table(self.mdb_file, "tDirs")

        for pattern in reader:
            self.pattern_to_route[int(pattern["ID"])] = convert_route_id[pattern["nLine"]]
        buff.close()

    def map_services(self):
        buff, reader = dump_mdb_table(self.mdb_file, "tDayTypes")
        self.daytype_to_service = {int(i["ID"]): i["nName"].upper().strip() for i in reader}
        buff.close()

    def load_trips(self):
        buff, reader = dump_mdb_table(self.mdb_file, "tDepts")

        for db_trip in reader:
            route_id = self.pattern_to_route[int(db_trip["nDir"])]
            service_id = self.daytype_to_service[int(db_trip["nDayType"])]
            start_time = "{:0>2}{:0>2}".format(*divmod(int(db_trip["nTime"]), 60))

            trip_id = "-".join([route_id, service_id, db_trip["nDir"], start_time])

            self.trips[int(db_trip["ID"])] = {
                "_times": [],
                "route_id": route_id,
                "service_id": service_id,
                "trip_id": trip_id,
                "trip_headsign": "",
            }

        buff.close()

    def load_times(self):
        buff, reader = dump_mdb_table(self.mdb_file, "tPassages")

        for db_dep in reader:
            trip_lookup_id = int(db_dep["nDept"])
            time = "{:0>2}:{:0>2}:00".format(*divmod(int(db_dep["nTime"]), 60))
            stop = int(db_dep["nStake"])

            if self.stops.check_id(stop):

                self.trips[trip_lookup_id]["_times"].append({
                    "arrival_time": time,
                    "departure_time": time,
                    "stop_id": stop,
                    "stop_sequence": int(db_dep["nOrder"]),
                })

        buff.close()

    def export_times(self):
        for trip in self.trips.values():
            times = sorted(trip.pop("_times"), key=lambda i: i["stop_sequence"])

            # Ignore empty trips
            if len(times) < 2:
                continue

            self.routes_used.add(trip["route_id"])
            self.services_used.add(trip["service_id"])

            # Add id prefix
            trip_id = self.id_prefix + trip["trip_id"]
            trip["trip_id"] = trip_id
            trip["service_id"] = self.id_prefix + trip["service_id"]

            # Generate trip_headsign
            last_stop_id = times[-1]["stop_id"]
            last_stop_name = self.stops.data[last_stop_id]["stop_name"]
            trip["trip_headsign"] = last_stop_name

            # Write to trips.txt
            self.trips_wrtr.writerow(trip)

            # Write to stop_times.txt
            for idx, stop_time in enumerate(times):
                stop_time["trip_id"] = trip_id
                stop_time["stop_sequence"] = idx
                self.stops.use_id(stop_time["stop_id"])

                self.times_wrtr.writerow(stop_time)

    def export_dates(self):
        if not self.services_used.issubset(KNOWN_SERVICES):
            raise ValueError(f"database {self.mdb_file!r} uses "
                             f"unknown calendars ({self.services_used})")

        if not self.services_used.issuperset({"POWSZEDNI", "SOBOTA", "NIEDZIELA"}):
            warn(f"databse {self.mdb_file!r} is not using major services "
                 f"(Powszedni, Sobota, Niedziela), only {self.services_used}")

        current_day = self.start_date

        while current_day <= self.end_date:
            date_str = current_day.strftime("%Y%m%d")

            # Holidays & Sundays
            if current_day.weekday == 6 or self.cal_exceptions.get(current_day) == "holiday":

                if "NIEDZIELA" in self.services_used:
                    self.dates_wrtr.writerow([self.id_prefix + "NIEDZIELA", date_str, 1])

            # Saturdays
            elif current_day.weekday == 5:

                if "SOBOTA" in self.services_used:
                    self.dates_wrtr.writerow([self.id_prefix + "SOBOTA", date_str, 1])

            # Workdays
            else:

                if "POWSZEDNI" in self.services_used:
                    self.dates_wrtr.writerow([self.id_prefix + "POWSZEDNI", date_str, 1])

            current_day += timedelta(days=1)

    # Main Function

    @classmethod
    def create(cls, target="radom.zip", force=False, fp_name=None, fp_url=None):
        print("Listing files from mzdik.radom.pl")
        files = list_files()

        data_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        feed_version = "/".join((i["version"] for i in files))

        print("\033[1A\033[K" "Syncing files")
        something_changed = get_files(files)

        if not (force or something_changed):
            print("\033[1A\033[K" "No new files, terminating")
            return

        self = cls()

        print("\033[1A\033[K" "Clearing gtfs/ directory")
        clear_directory("gtfs")

        print("\033[1A\033[K" "Loading stops")
        self.stops.read_data_mybus()

        print("\033[1A\033[K" "Opening trips/stop_times/calendar_dates")
        self.open_files()

        print("")

        for file_info in files:
            print("\033[2A\033[K" f"Parsing {file_info['path']}")
            print("\033[K" "Resetting file data")
            self.new_file(
                file_name=file_info["path"], id_prefix=file_info["version"] + ":",
                start_date=file_info["start"], end_date=file_info["end"],
            )

            print("\033[1A\033[K" "Loading table tStakes (stops)")
            self.stops.read_table(self.mdb_file)

            print("\033[1A\033[K" "Loading table tDirs (patterns)")
            self.map_patterns()

            print("\033[1A\033[K" "Loading table tDayTypes (services)")
            self.map_services()

            print("\033[1A\033[K" "Loading table tDepts (trips)")
            self.load_trips()

            print("\033[1A\033[K" "Loading table tPassages (stop_times)")
            self.load_times()

            print("\033[1A\033[K" "Exporting trips & stop_times")
            self.export_times()

            print("\033[1A\033[K" "Exporting calendar_dates")
            self.export_dates()

        print("\033[2A\033[K" "Closing trip/stop_times/calendar_dates")
        print("\033[K", end="")
        self.close()

        print("\033[1A\033[K" "Exporting stops")
        self.stops.export()

        print("\033[1A\033[K" "Exporting routes")
        self.routes()

        print("\033[1A\033[K" "Validating used services")
        self.verify_services()

        print("\033[1A\033[K" "Saving static files")
        self.static_files(feed_version, data_update, fp_name, fp_url)

        print("\033[1A\033[K" "Compressing")
        compress(target)

if __name__ == "__main__":
    st = time.time()
    argprs = argparse.ArgumentParser()
    argprs.add_argument("-o", "--output-file", default="radom.zip",
                        required=False, metavar="(path)", dest="target",
                        help="destination of the gtfs file (defualt: radom.zip)")
    argprs.add_argument("-f", "--force", action="store_true", required=False,
                        help="force the creation of GTFS even if no new files were downloaded")

    argprs.add_argument("-pn", "--publisher-name", required=False, metavar="NAME",
                        dest="publisher_name", help="value of feed_publisher_name")
    argprs.add_argument("-pu", "--publisher-url", required=False, metavar="URL",
                        dest="publisher_url", help="value of feed_publisher_url")

    args = argprs.parse_args()

    print("=== RadomGTFS ===")
    RadomGtfs.create(args.target, args.force, args.publisher_name, args.publisher_url)

    et = time.time() - st
    print(f"=== Done in {et:.2f} s ===")
