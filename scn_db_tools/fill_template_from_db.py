# The MIT License (MIT)
# Copyright (c) 2025 by the xcube team
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
import warnings
from typing import List

import click
from geopandas import GeoDataFrame
from pandas import ExcelWriter
from xcube_geodb.core.geodb import GeoDBClient


class DataFetcher:
    def __init__(
        self,
        geodb: GeoDBClient,
    ):
        self._geodb = geodb

    def fetch_data(
        self, unique_site_ids: List[str]
    ) -> dict[str, None] | dict[str, GeoDataFrame]:
        short_site_ids = [i[:4] for i in unique_site_ids]
        primary_tts = [i[5:] for i in unique_site_ids]
        sites_where = ""
        for i, ssid in enumerate(short_site_ids):
            sites_where += f"(short_site_id = '{ssid}' AND primary_target_type_id like '{primary_tts[i]}%')"
            sites_where += " OR "
        sites_where = sites_where[:-4]

        sites = self._geodb.get_collection_pg(
            collection="calibration_sites", where=sites_where, database="sarcalnet"
        )

        if sites is None or sites.empty:
            return {
                "sites": None,
                "targets": None,
                "surveys": None,
                "nat_targets": None,
                "nat_surveys": None,
            }

        targets_where = ""
        for row in sites.iterrows():
            site_name = row[1]["site_name"]
            targets_where += f"site_name = '{site_name}'"
            targets_where += " OR "
        targets_where = targets_where[:-4]

        targets = self._geodb.get_collection_pg(
            collection="calibration_targets", where=targets_where, database="sarcalnet"
        )

        nat_targets = self._geodb.get_collection_pg(
            collection="calibration_nat_targets",
            where=targets_where,
            database="sarcalnet",
        )

        if targets is not None:
            surveys_where = ""
            for row in targets.iterrows():
                target_id = row[1]["target_id"]
                surveys_where += f"target_id = '{target_id}'"
                surveys_where += " OR "
            surveys_where = surveys_where[:-4]

            surveys = self._geodb.get_collection_pg(
                collection="calibration_surveys",
                where=surveys_where,
                database="sarcalnet",
            )
        else:
            surveys = None

        if nat_targets is not None and not nat_targets.empty:
            nat_surveys_where = ""
            for row in nat_targets.iterrows():
                target_id = row[1]["target_id"]
                nat_surveys_where += f"target_id = '{target_id}'"
                nat_surveys_where += " OR "
            nat_surveys_where = nat_surveys_where[:-4]

            nat_surveys = self._geodb.get_collection_pg(
                collection="calibration_nat_surveys",
                where=nat_surveys_where,
                database="sarcalnet",
            )
        else:
            nat_surveys = None

        return {
            "sites": sites,
            "targets": targets,
            "surveys": surveys,
            "nat_targets": nat_targets
            if nat_targets is not None and not nat_targets.empty
            else None,
            "nat_surveys": nat_surveys
            if nat_surveys is not None and not nat_surveys.empty
            else None,
        }


class Outputter:
    def __init__(self):
        self.sites_columns = [
            "Unique Site ID",
            "Short Site ID",
            "Country",
            "Site Name",
            "Province / state / region",
            "Primary Target Type ID",
            "Target Types",
            "Primary Sensor",
            "Willing to consider special requests",
            "Responsible Organization",
            "Website",
            "Active from (YYYY-MM-DD)",
            'Active until (YYYY-MM-DD or "-")',
            "POC Name",
            "POC email",
            "Additional POC Name",
            "Additional POC email",
            "Centroid of the site (latitude and longitude in decima deg)",
            "Boundaries",
            "Planned maintenance schedule",
            "Characteristics",
        ]
        self.cr_columns = [
            "Unique Target ID",
            "Short Site ID",
            "Site Name",
            "Sub-site",
            "Internal ID",
            "Short Target ID",
            "Target Type ID",
            "Target description",
            "Approximate Latitude\n(decimal deg, WGS84)",
            "Approximate Longitude\n(decimal deg, WGS84)",
            "Approximate elevation\n(meters, WGS84)",
            "Approximate Azimuth angle\n(decimal deg)",
            "Approximate Boresight angle\n(decimal deg)",
            "Primary direction",
            "Side length (m)",
            "Photo link",
            "Operational",
            "Manufacturer",
            "Purpose of target",
            "Reference RCS (dBm2)",
            "Reference RCS measurement sensor",
            "Reference RCS measurement expected accuracy (dB)",
            "Reference RCS measurement boresite angle (decimal deg)",
            "Reference RCS measurement wavelength (m)",
            "Reference RCS measurement bandwidth (Hz)",
            "RCS accuracy determination method",
            "RCS angle dependency availablity",
            "Composition",
            "Characterization of reflector",
        ]
        self.survey_columns = [
            "Unique Target ID",
            "Survey date (YYYY-MM-DD)",
            "Latitude (decimal deg)",
            "Longitude (decimal deg)",
            "Elevation (m)",
            "Position accuracy (cm)",
            "Coordinate Reference System (WKT or EPSG)",
            "CRS X velocity (mm/year)",
            "CRS Y velocity (mm/year)",
            "CRS Z velocity (mm/year)",
            "Azimuth angle\n(decimal deg)",
            "Boresight angle\n(decimal deg)",
            "Tilt (decimal deg)",
            "Pointing accuracy\n(decimal deg)",
            "Fence",
            "Measurement method",
            "Offset method",
            "Applied corrections",
            "GNSS measurement duration\n(hh:mm:ss)",
            "Photo link",
            "Status report",
        ]

        self.dt_columns = [
            "Unique Target ID",
            "Short Site ID",
            "Site Name",
            "Sub-site",
            "Internal ID",
            "Short Target ID",
            "Target Type ID",
            "Target description",
            "Bounding polygon (WKT, WGS84)",
            "Coverage (km2)",
            "Mask polygon (WKT, WGS84)",
            "Start Monitoring Period (YYYY-MM-DD)",
            'Stop Monitoring Period (YYYY-MM-DD or "-")',
        ]

        self.nat_survey_columns = [
            "Unique Target ID",
            "Start Survey Period (YYYY-MM-DD)",
            "Stop Survey Period (YYYY-MM-DD)",
            "Mission",
            "Carrier Frequency (GHz)",
            "Polarization Channel",
            "UTC Observation Time (HH:MM)",
            "Local Observation time (HH:MM)",
            "Incidence Angle Range (min - max, in decimal deg)",
            "Backscatter coefficient type",
            "Mean Backscatter Coefficient (dB)",
            "Backscatter Coefficient Standard Deviation (dB)",
            "Reference Surface",
            "Samples",
            "Relative Orbit",
            "Orbit direction",
            "Look side",
            "Acquisition Mode",
            "Beam ID",
            "Scene identifier(s)",
            "Query URL",
        ]

    def write_form(
        self, data: dict[str, None] | dict[str, GeoDataFrame], target_file: str
    ):
        with ExcelWriter(target_file, "openpyxl") as excel_writer:
            sites: GeoDataFrame = data["sites"]
            sites["unique_site_id"] = (
                sites["short_site_id"]
                + "-"
                + sites["primary_target_type_id"].astype(str).str[:2]
            )
            sites.insert(0, "unique_site_id", sites.pop("unique_site_id"))
            sites.rename(
                columns={
                    "unique_site_id": "Unique Site ID",
                    "short_site_id": "Short Site ID",
                    "country": "Country",
                    "site_name": "Site Name",
                    "province_state_region": "Province / state / region",
                    "primary_target_type_id": "Primary Target Type ID",
                    "target_types": "Target Types",
                    "primary_sensor": "Primary Sensor",
                    "special_requests": "Willing to consider special requests",
                    "responsible_organization": "Responsible Organization",
                    "website": "Website",
                    "active_from": "Active from (YYYY-MM-DD)",
                    "active_until": 'Active until (YYYY-MM-DD or "-")',
                    "poc_name": "POC Name",
                    "poc_email": "POC email",
                    "poc_name2": "Additional POC Name",
                    "poc_email2": "Additional POC email",
                    "centroid": "Centroid of the site (latitude and longitude in decima deg)",
                    "boundaries": "Boundaries",
                    "maintenance_schedule": "Planned maintenance schedule",
                    "landcover": "Characteristics",
                },
                inplace=True,
            )

            sites.to_excel(
                excel_writer, sheet_name="site", columns=self.sites_columns, index=False
            )
            targets: GeoDataFrame = data["targets"]
            if targets is not None:
                targets.rename(
                    columns={
                        "target_id": "Unique Target ID",
                        "short_site_id": "Short Site ID",
                        "site_name": "Site Name",
                        "subsite": "Sub-site",
                        "internal_id": "Internal ID",
                        "short_target_id": "Short Target ID",
                        "target_type": "Target Type ID",
                        "target_description": "Target description",
                        "approx_lat": "Approximate Latitude\n(decimal deg, WGS84)",
                        "approx_lon": "Approximate Longitude\n(decimal deg, WGS84)",
                        "approx_h": "Approximate elevation\n(meters, WGS84)",
                        "approx_azimuth_angle": "Approximate Azimuth angle\n(decimal deg)",
                        "approx_boresight_angle": "Approximate Boresight angle\n(decimal deg)",
                        "primary_direction": "Primary direction",
                        "side_length": "Side length (m)",
                        "photo_link": "Photo link",
                        "operational": "Operational",
                        "manufacturer": "Manufacturer",
                        "purpose": "Purpose of target",
                        "rcs": "Reference RCS (dBm2)",
                        "rcs_measurement_conditions": "Reference RCS measurement sensor",
                        "reference_rcs_accuracy": "Reference RCS measurement expected accuracy (dB)",
                        "reference_rcs_boresight_angle": "Reference RCS measurement boresite angle (decimal deg)",
                        "reference_rcs_wavelength": "Reference RCS measurement wavelength (m)",
                        "reference_rcs_bandwidth": "Reference RCS measurement bandwidth (Hz)",
                        "rcs_accuracy_determination": "RCS accuracy determination method",
                        "rcs_angle_dependency_availability": "RCS angle dependency availablity",
                        "composition": "Composition",
                        "characterization": "Characterization of reflector",
                    },
                    inplace=True,
                )
                targets.to_excel(
                    excel_writer, sheet_name="cr", columns=self.cr_columns, index=False
                )
            surveys: GeoDataFrame = data["surveys"]
            if surveys is not None:
                surveys.rename(
                    columns={
                        "target_id": "Unique Target ID",
                        "survey_date": "Survey date (YYYY-MM-DD)",
                        "lat": "Latitude (decimal deg)",
                        "lon": "Longitude (decimal deg)",
                        "elevation": "Elevation (m)",
                        "position_accuracy": "Position accuracy (cm)",
                        "crs": "Coordinate Reference System (WKT or EPSG)",
                        "crs_vx": "CRS X velocity (mm/year)",
                        "crs_vy": "CRS Y velocity (mm/year)",
                        "crs_vz": "CRS Z velocity (mm/year)",
                        "azimuth_angle": "Azimuth angle\n(decimal deg)",
                        "boresight_angle": "Boresight angle\n(decimal deg)",
                        "tilt_angle": "Tilt (decimal deg)",
                        "pointing_accuracy": "Pointing accuracy\n(decimal deg)",
                        "fence": "Fence",
                        "measurement_method": "Measurement method",
                        "offset_method": "Offset method",
                        "applied_corrections": "Applied corrections",
                        "gnss_measurement_duration": "GNSS measurement duration\n(hh:mm:ss)",
                        "photo_link": "Photo link",
                        "report_status": "Status report",
                    },
                    inplace=True,
                )
                surveys.to_excel(
                    excel_writer,
                    sheet_name="surveys",
                    columns=self.survey_columns,
                    index=False,
                )
            nat_targets: GeoDataFrame = data["nat_targets"]
            if nat_targets is not None:
                nat_targets.rename(
                    columns={
                        "target_id": "Unique Target ID",
                        "short_site_id": "Short Site ID",
                        "site_name": "Site Name",
                        "subsite": "Sub-site",
                        "internal_id": "Internal ID",
                        "short_target_id": "Short Target ID",
                        "target_type": "Target Type ID",
                        "target_description": "Target description",
                        "geometry": "Bounding polygon (WKT, WGS84)",
                        "coverage": "Coverage (km2)",
                        "mask_polygon": "Mask polygon (WKT, WGS84)",
                        "start_monitoring": "Start Monitoring Period (YYYY-MM-DD)",
                        "stop_monitoring": 'Stop Monitoring Period (YYYY-MM-DD or "-")',
                    },
                    inplace=True,
                )
                nat_targets.to_excel(
                    excel_writer, sheet_name="dt", columns=self.dt_columns, index=False
                )
            nat_surveys: GeoDataFrame = data["nat_surveys"]
            if "surveys" in excel_writer.sheets:
                sheet_name = "nat_surveys"
            else:
                sheet_name = "surveys"
            if nat_surveys is not None:
                nat_surveys.rename(
                    columns={
                        "target_id": "Unique Target ID",
                        "survey_start": "Start Survey Period (YYYY-MM-DD)",
                        "survey_stop": "Stop Survey Period (YYYY-MM-DD)",
                        "mission": "Mission",
                        "carrier_frequency": "Carrier Frequency (GHz)",
                        "polarizations": "Polarization Channel",
                        "observation_time": "UTC Observation Time (HH:MM)",
                        "local_observation_time": "Local Observation time (HH:MM)",
                        "incidence_angle_range": "Incidence Angle Range (min - max, in decimal deg)",
                        "backscatter_coefficient_type": "Backscatter coefficient type",
                        "backscatter_coeff_mean": "Mean Backscatter Coefficient (dB)",
                        "backscatter_coeff_std": "Backscatter Coefficient Standard Deviation (dB)",
                        "reference_surface": "Reference Surface",
                        "samples": "Samples",
                        "relative_orbit": "Relative Orbit",
                        "orbit_dir": "Orbit direction",
                        "look_side": "Look side",
                        "acquisition_mode": "Acquisition Mode",
                        "beam_id": "Beam ID",
                        "scene_ids": "Scene identifier(s)",
                        "query_url": "Query URL",
                    },
                    inplace=True,
                )
                nat_surveys.to_excel(
                    excel_writer,
                    sheet_name=sheet_name,
                    columns=self.nat_survey_columns,
                    index=False,
                )


@click.command()
@click.argument(
    "unique_site_id",
    metavar="UNIQUE_SITE_ID",
)
@click.argument(
    "target_file",
    metavar="TARGET_FILE",
)
@click.option("--client_id", required=True, help="The geoDB client_id.")
@click.option("--client_secret", required=True, help="The geoDB client_secret.")
@click.option(
    "--server_url",
    default="https://xcube-geodb.brockmann-consult.de",
    help="The geoDB server URL.",
)
@click.option("--server_port", default=443, help="The geoDB server port.")
@click.option(
    "--auth_domain",
    default="https://winchester.production.brockmann-consult.de/winchester",
    help="The geoDB auth domain URL.",
)
def create_form(
    unique_site_id: str,
    target_file: str,
    client_id: str,
    client_secret: str,
    server_url: str,
    server_port: int,
    auth_domain: str,
):
    """
    Creates a filled template from the information of the SARCalNet database.
    """

    warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
    geodb = GeoDBClient(
        server_url, server_port, client_id, client_secret, auth_domain=auth_domain
    )
    data = DataFetcher(geodb).fetch_data([unique_site_id])
    if not target_file.endswith(".xlsx"):
        target_file = target_file + ".xlsx"
    Outputter().write_form(data, target_file)


if __name__ == "__main__":
    create_form()
