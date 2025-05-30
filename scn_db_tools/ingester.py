import math
import os
import string
import urllib
import random
from typing import Optional, List, Tuple

import dateutil.parser
import re

import numpy as np
import pandas as pd
import geopandas as gpd
import pyproj
import requests
import shapely
from geopandas import GeoDataFrame

from pandas import DataFrame
from requests.auth import HTTPBasicAuth
from xcube_geodb.core.geodb import GeoDBClient


DATABASE = "sarcalnet"

SITES_COLLECTION = "calibration_sites"
TARGETS_COLLECTION = "calibration_targets"
NAT_TARGETS_COLLECTION = "calibration_nat_targets"
SURVEYS_COLLECTION = "calibration_surveys"
NAT_SURVEYS_COLLECTION = "calibration_nat_surveys"


class Ingester:
    def __init__(
        self,
        server_url: Optional[str] = None,
        server_port: Optional[int] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        auth_domain: Optional[str] = None,
        admin_password: Optional[str] = None,
        filebird_token: Optional[str] = None,
    ):
        self.admin_password = admin_password
        self.filebird_token = filebird_token

        if not client_id or not client_secret:
            self.validation_mode = True
        else:
            self.validation_mode = False

        if self.validation_mode:
            self.geoDB = None
        else:
            self.geoDB = GeoDBClient(
                server_url=server_url,
                server_port=server_port,
                client_id=client_id,
                client_secret=client_secret,
                auth_domain=auth_domain,
            )

    def create_collection(self):
        properties = {
            "short_site_identifier": "varchar",
            "sitename": "varchar",
            "country": "varchar",
            "province_state_region": "varchar",
            "primary_target_type_identifier": "varchar",
            "target_types": "varchar",
            "primary_sensor": "varchar",
            "special_requests": "boolean",
            "responsible_organization": "varchar",
            "website": "varchar",
            "active_from": "date",
            "active_until": "date",
            "poc_name": "varchar",
            "poc_mail": "varchar",
            "poc2_name": "varchar",
            "poc2_mail": "varchar",
            "centroid": "varchar",
            "boundaries": "varchar",
            "maintenance_schedule": "varchar",
            "characteristics": "varchar",
            "endorsement": "varchar",
        }
        if not self.geoDB.collection_exists(SITES_COLLECTION, DATABASE):
            self.geoDB.create_collection(
                SITES_COLLECTION, properties, database=DATABASE
            )

    def create_site_gdf(self, calibration_site_xls: str) -> Optional[GeoDataFrame]:
        sites_df = pd.read_excel(
            calibration_site_xls,
            "site",
            skiprows=range(1, 5),
            converters={
                "Active from  (YYYY-MM-DD)": str,
                "Active from (YYYY-MM-DD)": str,
                'Active until (YYYY-MM-DD or " - ")': str,
                'Active until (YYYY-MM-DD or "-")': str,
                "Planned maintenance schedule": lambda x: (
                    "Nothing" if x == "N/A" else x
                ),
            },
        )

        sites_df.rename(
            columns={
                "Short Site ID": "short_site_identifier",
                "Country": "country",
                "Site Name": "sitename",
                "Province / state / region": "province_state_region",
                "Primary Target Type ID": "primary_target_type_identifier",
                "Target Types": "target_types",
                "Primary Sensor": "primary_sensor",
                "Willing to consider special requests": "special_requests",
                "Responsible Organization": "responsible_organization",
                "Website": "website",
                "Active from  (YYYY-MM-DD)": "active_from",
                "Active from (YYYY-MM-DD)": "active_from",
                'Active until (YYYY-MM-DD or "-")': "active_until",
                "POC Name": "poc_name",
                "POC email": "poc_mail",
                "Additional POC Name": "poc2_name",
                "Additional POC email": "poc2_mail",
                "Centroid of the site (latitude and longitude in decima deg)": "centroid",
                "Boundaries": "boundaries",
                "Planned maintenance schedule": "maintenance_schedule",
                "Characteristics": "characteristics",
            },
            inplace=True,
        )

        sites_df = sites_df[sites_df["Unique Site ID"] != "-"]
        sites_df = sites_df[sites_df["Unique Site ID"].notna()]
        sites_df = sites_df.drop("Unique Site ID", axis=1)

        self.validate_sites(sites_df)
        if self.validation_mode:
            return None
        print("Sites validation successful.")

        sites_df = sites_df.replace(to_replace=np.nan, value="")
        sites_df = sites_df.replace(to_replace="-", value=None)

        sites_df = sites_df.apply(self.compute_centroid_from_boundaries, axis=1)

        sites_df.insert(len(sites_df.columns), "endorsement", "review")
        sites_df.insert(len(sites_df.columns), "geometry", "POINT(0 0)")
        sites_df["geometry"] = gpd.GeoSeries.from_wkt(sites_df["boundaries"])
        return gpd.GeoDataFrame(sites_df, geometry="geometry", crs=4326)

    def update_sites(self, calibration_site_xls: str):
        gdf = self.create_site_gdf(calibration_site_xls)
        existing_sites = self.geoDB.get_collection(
            SITES_COLLECTION,
            "select=short_site_identifier,primary_target_type_identifier",
            database=DATABASE,
        )
        existing_site_ids = [
            a[:7]
            for a in list(
                existing_sites["short_site_identifier"]
                + "-"
                + existing_sites["primary_target_type_identifier"]
            )
        ]
        # only update existing sites, do not create new sites
        # for site_id in existing_site_ids:
        #     gdf = gdf[
        #         (gdf["short_site_identifier"] == site_id[:4])
        #         & (gdf["primary_target_type_identifier"] == site_id[4:])
        #     ]

        gdf = gdf[
            (
                gdf["short_site_identifier"]
                + "_"
                + gdf["primary_target_type_identifier"]
            ).isin(existing_site_ids)
        ]

        return self.do_site_ingestion(gdf)

    def ingest_sites(
        self, calibration_site_xls: str, license_url: Optional[str] = None
    ) -> List[str]:

        gdf = self.create_site_gdf(calibration_site_xls)
        if gdf is None:
            return []

        gdf.insert(len(gdf.columns), "license_url", license_url)
        return self.do_site_ingestion(gdf)

    def do_site_ingestion(self, gdf):
        if len(gdf) > 0:
            self.geoDB.insert_into_collection(
                SITES_COLLECTION, gdf, database=DATABASE, crs=4326
            )
            print(f"Successfully ingested {len(gdf)} sites.")
            return list(gdf["short_site_identifier"])
        else:
            print("No new sites ingested.")
        return []

    def validate_sites(self, sites_df):
        mandatory_fields = [
            "short_site_identifier",
            "sitename",
            "country",
            "primary_target_type_identifier",
            "target_types",
            "primary_sensor",
            "special_requests",
            "responsible_organization",
            "active_from",
            "active_until",
            "poc_name",
            "poc_mail",
            "boundaries",
            "maintenance_schedule",
            "characteristics",
        ]
        for row in sites_df.iterrows():
            for col in mandatory_fields:
                if str(row[1][col]) == "nan":
                    message = (
                        f"site, row {row[0] + 6}: missing entry for mandatory field "
                        f"'{col}'. Please fill in all mandatory fields,"
                        f" or remove the entire row."
                    )
                    if self.validation_mode:
                        print(message)
                    else:
                        raise ValueError(message)
            try:
                dateutil.parser.parse(row[1]["active_from"])
            except dateutil.parser.ParserError:
                message = (
                    f"site, row {row[0] + 6}: invalid entry for field "
                    f"'active_from'. Please provide a date or date and time expressed "
                    f"in YYYY-MM-dd format (UTC)."
                )
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)
            try:
                active_until = row[1]["active_until"]
                if not (active_until == "-" or active_until == "'-'"):
                    dateutil.parser.parse(active_until)
            except dateutil.parser.ParserError:
                message = (
                    f"site, row {row[0] + 6}: invalid entry for field "
                    f"'active_until'. Please provide a date or date and time expressed "
                    f"in YYYY-MM-dd format (UTC), or provide '-'. "
                )
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)
            try:
                website = row[1]["website"]
                if website and not str(website) == "nan":
                    if website.startswith("http"):
                        if requests.get(website).status_code >= 400:
                            raise requests.exceptions.ConnectionError
                    else:
                        if (
                            requests.get("http://" + website).status_code >= 400
                            or requests.get("https://" + website).status_code >= 400
                        ):
                            raise requests.exceptions.ConnectionError
            except requests.exceptions.ConnectionError:
                message = (
                    f"site, row {row[0] + 6}: invalid entry for field "
                    f"'website'. Please provide an accessible website."
                )
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)

    def update_targets(self, calibration_site_xls: str) -> Tuple[str, List[str]]:
        targets = self.read_targets(calibration_site_xls)
        existing_targets = self.geoDB.get_collection(
            TARGETS_COLLECTION,
            "select=unique_target_id",
            database=DATABASE,
        )
        existing_target_ids = list(existing_targets["unique_target_id"])

        nat_targets_df = targets[0]
        art_targets_df = targets[1]
        art_targets_unavailability = targets[2]
        for target_id in existing_target_ids:
            if nat_targets_df is not None:
                nat_targets_df = nat_targets_df[
                    nat_targets_df["Unique Target ID"] != target_id
                ]
            if art_targets_df is not None:
                art_targets_df = art_targets_df[
                    art_targets_df["Unique Target ID"] != target_id
                ]
            if art_targets_unavailability is not None:
                art_targets_unavailability = art_targets_unavailability[
                    art_targets_unavailability["Unique Target ID"] != target_id
                ]

        updated_site_ids = []
        updated_site_ids += (
            list(nat_targets_df["Short Site ID"].dropna())
            if nat_targets_df is not None
            and nat_targets_df["Short Site ID"].dropna() is not None
            else []
        )
        updated_site_ids += (
            list(art_targets_df["Short Site ID"].dropna())
            if art_targets_df is not None
            and art_targets_df["Short Site ID"].dropna() is not None
            else []
        )

        return (
            self.do_targets_ingestion(
                (nat_targets_df, art_targets_df, art_targets_unavailability)
            ),
            updated_site_ids,
        )

    def read_targets(
        self, calibration_site_xls: str
    ) -> tuple[Optional[DataFrame], Optional[DataFrame], Optional[DataFrame]]:
        nat_targets_df = None
        art_targets_df = None
        art_targets_unavailability = None
        try:
            nat_targets_df = pd.read_excel(
                calibration_site_xls,
                "dt",
                skiprows=range(1, 5),
                converters={
                    "Start Monitoring Period (YYYY-MM-DD)": str,
                    'Stop Monitoring Period (YYYY-MM-DD or "-")': str,
                },
            )
        except ValueError:
            art_targets_df = pd.read_excel(
                calibration_site_xls, "cr", skiprows=range(1, 5)
            )
            try:
                art_targets_unavailability = pd.read_excel(
                    calibration_site_xls,
                    "unavailability (optional)",
                    skiprows=range(1, 5),
                    converters={
                        "Start of Unavailability (YYYY-MM-DD)": str,
                        "End of Unavailability (YYYY-MM-DD)": str,
                    },
                )
            except ValueError as exc:
                if str(exc) == "Worksheet named 'unavailability (optional)' not found":
                    art_targets_unavailability = None
                else:
                    raise exc
        return nat_targets_df, art_targets_df, art_targets_unavailability

    def ingest_targets(self, calibration_site_xls: str) -> str:
        targets = self.read_targets(calibration_site_xls)
        return self.do_targets_ingestion(targets)

    def do_targets_ingestion(self, targets):
        nat_targets_df = targets[0]
        art_targets_df = targets[1]
        art_targets_unavailability = targets[2]
        if nat_targets_df is not None:
            self.ingest_nat_targets(nat_targets_df)
            return "natural"
        elif art_targets_df is not None:
            self.ingest_art_targets(art_targets_df, art_targets_unavailability)
            return "artificial"
        else:
            print("Could neither read dt nor cr. No targets ingested.")
            return "None"

    def ingest_nat_targets(self, targets_df: pd.DataFrame):
        targets_df.rename(
            columns={
                "Unique Target ID": "unique_target_id",
                "Short Site ID": "short_site_identifier",
                "Site Name": "sitename",
                "Sub-site": "subsite",
                "Internal ID": "internal_id",
                "Short Target ID": "short_target_id",
                "Target Type ID": "target_type_id",
                "Target description": "target_description",
                "Bounding polygon (WKT, WGS84)": "geometry",
                "Coverage (km2)": "coverage",
                "Mask polygon (WKT, WGS84)": "mask_polygon",
                "Start Monitoring Period (YYYY-MM-DD)": "period_start",
                'Stop Monitoring Period (YYYY-MM-DD or "-")': "period_stop",
            },
            inplace=True,
            errors="ignore",
        )
        targets_df = targets_df[targets_df["unique_target_id"] != "--"]
        targets_df = targets_df.replace(math.nan, None)
        targets_df = targets_df.replace(to_replace="-", value=None)

        self.validate_nat_targets(targets_df)
        if self.validation_mode:
            return
        print("Targets validation successful.")

        gdf = gpd.GeoDataFrame(targets_df)

        if len(gdf) > 0:
            self.geoDB.insert_into_collection(
                NAT_TARGETS_COLLECTION, gdf, database=DATABASE, crs=4326
            )
            print(f"Successfully ingested {len(gdf)} natural targets.")
        else:
            print("No targets ingested.")

    def validate_nat_targets(self, nat_targets_df):
        mandatory_fields = [
            "unique_target_id",
            "short_site_identifier",
            "sitename",
            "short_target_id",
            "target_type_id",
            "geometry",
            "period_start",
            "period_stop",
        ]
        for row in nat_targets_df.iterrows():
            for col in mandatory_fields:
                if str(row[1][col]) == "nan":
                    message = (
                        f"dt, row {row[0] + 6}: missing entry for "
                        f"mandatory field '{col}'. Please fill in all"
                        f" mandatory fields, "
                        f"or remove the entire row."
                    )
                    if self.validation_mode:
                        print(message)
                    else:
                        raise ValueError(message)

    def ingest_art_targets(
        self, targets_df: pd.DataFrame, unavailability: Optional[DataFrame]
    ) -> None:
        targets_df.rename(
            columns={
                "Unique Target ID": "unique_target_id",
                "Short Site ID": "short_site_identifier",
                "Site Name": "sitename",
                "Sub-site": "subsite",
                "Internal ID": "internal_id",
                "Short Target ID": "short_target_id",
                "Target Type ID": "target_type_id",
                "Target description": "target_description",
                "Approximage Latitude\n(decimal deg, WGS84)": "apprx_latitude",
                "Approximate Latitude\n(decimal deg, WGS84)": "apprx_latitude",
                "Approximate Longitude\n(decimal deg WGS84)": "apprx_longitude",
                "Approximate Longitude\n(decimal deg, WGS84)": "apprx_longitude",
                "Approximate elevation\n(meters, WGS84)": "apprx_elevation",
                "Approximate Azimuth angle\n(decimal deg)": "apprx_azimuth",
                "Approximate Boresight angle\n(decimal deg)": "apprx_boresight",
                "Primary direction": "primary_direction",
                "Side length (m)": "side_length",
                "Photo link": "photo_link",
                "Operational": "operational",
                "Manufacturer": "manufacturer",
                "Purpose of target": "purpose",
                "Reference RCS (dBm2)": "rcs",
                "Reference RCS measurement sensor": "rcs_sensor",
                "Reference RCS measurement expected accuracy (dB)": "rcs_accuracy",
                "Reference RCS measurement boresite angle (decimal deg)": "rcs_angle",
                "Reference RCS measurement wavelength (m)": "rcs_wavelength",
                "Reference RCS measurement bandwidth (Hz)": "rcs_bandwidth",
                "RCS accuracy determination method": "rcs_method",
                "RCS angle dependency availablity": "rcs_angle_dependency",
                "Composition": "composition",
                "Characterization of reflector ": "characterization",
                "Characterization of reflector": "characterization",
            },
            inplace=True,
            errors="ignore",
        )
        targets_df = targets_df[targets_df["unique_target_id"] != "--"]
        targets_df = targets_df.replace(math.nan, None)

        self.validate_art_targets(targets_df)
        if self.validation_mode:
            return
        print("Targets validation successful.")

        self.upload_photos(targets_df)

        if unavailability is not None:
            unavailability.rename(
                columns={
                    "Unique Target ID": "unique_target_id",
                    "Start of Unavailability (YYYY-MM-DD)": "unavailability_start",
                    "End of Unavailability (YYYY-MM-DD)": "unavailability_end",
                },
                inplace=True,
                errors="ignore",
            )
            unavailability = unavailability.drop("Unique Site ID", axis=1)
            unavailability = unavailability.drop("Internal ID", axis=1)
            unavailability = unavailability.groupby(
                "unique_target_id", as_index=False
            ).agg({"unavailability_start": list, "unavailability_end": list})
            targets_df = targets_df.merge(unavailability, on="unique_target_id")

        gdf = gpd.GeoDataFrame(
            targets_df,
            geometry=gpd.points_from_xy(
                targets_df.apprx_longitude, targets_df.apprx_latitude
            ),
            crs=4326,
        )

        if len(gdf) > 0:
            self.geoDB.insert_into_collection(
                TARGETS_COLLECTION, gdf, database=DATABASE, crs=4326
            )
            print(f"Successfully ingested {len(gdf)} artificial targets.")
        else:
            print("No targets ingested.")

    def validate_art_targets(self, art_targets_df):
        mandatory_fields = [
            "unique_target_id",
            "short_site_identifier",
            "short_target_id",
            "target_type_id",
            "apprx_latitude",
            "apprx_longitude",
            "apprx_elevation",
            "apprx_azimuth",
            "apprx_boresight",
            "primary_direction",
            "side_length",
            "operational",
            "purpose",
        ]
        for row in art_targets_df.iterrows():
            for col in mandatory_fields:
                if str(row[1][col]) == "nan":
                    message = (
                        f"cr, row {row[0] + 6}: missing entry for mandatory field "
                    )
                    f"'{col}'. Please fill in all mandatory fields, "
                    f"or remove the entire row."
                    if self.validation_mode:
                        print(message)
                    else:
                        raise ValueError(message)
            try:
                float(row[1]["apprx_latitude"])
            except ValueError:
                message = f"cr, row {row[0] + 6}: invalid entry for mandatory field "
                f"'Approximate Latitude'. Please enter a valid "
                f"floating point number."
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)
            if row[1]["apprx_latitude"] < -90 or row[1]["apprx_latitude"] > 90:
                message = f"cr, row {row[0] + 6}: invalid entry for mandatory field "
                f"'Approximate Latitude'. Please enter a valid "
                f"floating point number > -90 and < 90. "
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)
            try:
                float(row[1]["apprx_longitude"])
            except ValueError:
                message = f"cr, row {row[0] + 6}: invalid entry for mandatory field "
                f"'Approximate Longitude'. Please enter a valid "
                f"floating point number."
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)
            if row[1]["apprx_longitude"] < -180 or row[1]["apprx_longitude"] > 180:
                message = f"cr, row {row[0] + 6}: invalid entry for mandatory field "
                f"'Approximate Longitude'. Please enter a valid "
                f"floating point number > -180 and < 180."
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)
            try:
                float(row[1]["apprx_elevation"])
            except ValueError:
                message = f"cr, row {row[0] + 6}: invalid entry for mandatory field "
                f"'Approximate Elevation'. Please enter a valid "
                f"floating point number."
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)
            try:
                float(row[1]["side_length"])
                if row[1]["side_length"] <= 0:
                    raise ValueError
            except ValueError:
                message = (
                    f"cr, row {row[0] + 6}: invalid entry for mandatory field "
                    f"'Side Length'. Please enter a valid "
                    f"floating point number > 0."
                )
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)
            try:
                if row[1]["rcs"]:
                    float(row[1]["rcs"])
            except ValueError:
                message = (
                    f"cr, row {row[0] + 6}: invalid entry for field "
                    f"'RCS'. Please enter a valid "
                    f"floating point number."
                )
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)
            try:
                if row[1]["rcs_accuracy"]:
                    float(row[1]["rcs_accuracy"])
            except ValueError:
                message = (
                    f"cr, row {row[0] + 6}: invalid entry for field "
                    f"'Reference RCS measurement expected accuracy (dB)'. "
                    f"Please enter a valid floating point number."
                )
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)
            try:
                if row[1]["rcs_angle"]:
                    float(row[1]["rcs_angle"])
            except ValueError:
                message = (
                    f"cr, row {row[0] + 6}: invalid entry for field "
                    f"'Reference RCS measurement boresite angle (decimal deg)'. "
                    f"Please enter a valid floating point number."
                )
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)
            try:
                if row[1]["rcs_wavelength"] and row[1]["rcs_wavelength"] <= 0:
                    raise ValueError
            except ValueError:
                message = (
                    f"cr, row {row[0] + 6}: invalid entry for field "
                    f"'Reference RCS measurement wavelength (m)'. Please enter a "
                    f"valid floating point number > 0."
                )
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)
            try:
                if row[1]["rcs_bandwidth"] and row[1]["rcs_bandwidth"] <= 0:
                    raise ValueError
            except ValueError:
                message = (
                    f"cr, row {row[0] + 6}: invalid entry for field "
                    f"'Reference RCS measurement bandwidth (Hz)'. Please enter"
                    f" a valid floating point number > 0."
                )
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)

    def create_surveys_collection(self):
        properties = {
            "unique_target_id": "varchar",
            "survey_date": "date",
            "lat": "float",
            "lon": "float",
            "elevation": "float",
            "position_accuracy": "float",
            "coordinate_reference_system": "varchar",
            "azimuth_angle": "float",
            "boresight_angle": "float",
            "tilt": "float",
            "accuracy": "float",
            "fence": "boolean",
            "measurement_method": "varchar",
            "offset_method": "varchar",
            "corrections": "varchar",
            "duration": "varchar",
            "photo_link": "varchar",
            "status_report": "varchar",
        }

        if not self.geoDB.collection_exists(SURVEYS_COLLECTION, DATABASE):
            self.geoDB.create_collection(
                SURVEYS_COLLECTION, properties, database=DATABASE
            )
            print("Created surveys table.")

    def ingest_surveys(self, calibration_site_xls: str, target_type: str):
        if target_type == "natural":
            self.ingest_nat_surveys(calibration_site_xls)
        elif target_type == "artificial":
            self.ingest_art_surveys(calibration_site_xls)
        else:
            print("No surveys ingested.")

    def update_surveys(self, calibration_site_xls: str, target_type: str):
        if target_type == "natural":
            self.update_nat_surveys(calibration_site_xls)
        elif target_type == "artificial":
            self.update_art_surveys(calibration_site_xls)
        else:
            print(f"Invalid target type {target_type}. No surveys ingested.")

    def update_nat_surveys(self, calibration_site_xls: str):
        surveys_df = self.read_nat_surveys(calibration_site_xls)
        if surveys_df is None:
            return
        existing_surveys = self.geoDB.get_collection(
            NAT_SURVEYS_COLLECTION,
            "select=unique_target_id,survey_date",
            database=DATABASE,
        )
        existing_survey_ids = []
        dates = list(existing_surveys["survey_date"])
        for i, target_id in enumerate(list(existing_surveys["unique_target_id"])):
            existing_survey_ids.append(target_id + "_" + dates[i])
        print(existing_survey_ids)

    def update_art_surveys(self, calibration_site_xls: str):
        surveys_df = self.read_art_surveys(calibration_site_xls)
        if surveys_df is None:
            return
        existing_surveys = self.geoDB.get_collection(
            SURVEYS_COLLECTION,
            "select=unique_target_id,survey_date",
            database=DATABASE,
        )
        existing_survey_ids = []
        dates = list(existing_surveys["survey_date"])
        for i, target_id in enumerate(list(existing_surveys["unique_target_id"])):
            existing_survey_ids.append(target_id + "_" + dates[i])
        print(existing_survey_ids)

    def ingest_nat_surveys(self, calibration_site_xls: str) -> None:
        surveys_df = self.read_nat_surveys(calibration_site_xls)
        if surveys_df is None:
            return
        surveys_df.insert(len(surveys_df.columns), "geometry", "POINT(0 0)")
        surveys_df["geometry"] = gpd.GeoSeries.from_wkt(surveys_df["geometry"])
        gdf = gpd.GeoDataFrame(surveys_df, geometry="geometry", crs=4326)

        if len(gdf) > 0:
            self.geoDB.insert_into_collection(
                NAT_SURVEYS_COLLECTION, gdf, database=DATABASE, crs=4326
            )
            print(f"Successfully ingested {len(gdf)} surveys of natural targets.")
        else:
            print("No new surveys ingested.")

    def read_nat_surveys(self, calibration_site_xls: str) -> Optional[DataFrame]:
        surveys_df = pd.read_excel(
            calibration_site_xls,
            "survey",
            skiprows=range(1, 5),
            converters={
                "Start Survey Period (YYYY-MM-DD)": str,
                "Stop Survey Period (YYYY-MM-DD)": str,
                "UTC Observation Time (HH:MM)": str,
                "Local Observation time (HH:MM)": str,
            },
        )
        surveys_df.rename(
            columns={
                "Unique Target ID": "unique_target_id",
                "Start Survey Period (YYYY-MM-DD)": "survey_start",
                "Stop Survey Period (YYYY-MM-DD)": "survey_stop",
                "Mission": "mission",
                "Carrier Frequency (GHz)": "carrier_frequency",
                "Polarization Channels": "polarization_channels",
                "UTC Observation Time (HH:MM)": "observation_time_utc",
                "Local Observation time (HH:MM)": "observation_time_local",
                "Incidence Angle Range (min - max, in decimal deg)": "incidence_angle_range",
                "Backscatter coefficient type": "backscatter_coefficient_type",
                "Mean Backscatter Coefficient (dB)": "backscatter_coefficient_mean",
                "Backscatter Coefficient Standard Deviation (dB)": "backscatter_coefficient_std",
                "Reference Surface": "reference_surface",
                "Samples": "samples",
                "Relative Orbit": "relative_orbit",
                "Orbit direction": "orbit_direction",
                "Look side": "look_side",
                "Acquisition Mode": "acquisition_mode",
                "Beam ID": "beam_id",
                "Scene identifier(s)": "scene_identifier",
                "Query URL": "query_url",
            },
            inplace=True,
            errors="ignore",
        )

        surveys_df = surveys_df[surveys_df["unique_target_id"] != ""]
        surveys_df = surveys_df.replace(math.nan, None)

        self.validate_nat_surveys(surveys_df)
        print("Successfully validated surveys.")
        if self.validation_mode:
            return None

        return surveys_df.replace(to_replace="-", value=None)

    def ingest_art_surveys(self, calibration_site_xls: str):
        gdf = self.read_art_surveys(calibration_site_xls)
        if gdf is None:
            return
        self.upload_photos(gdf)

        if len(gdf) > 0:
            self.geoDB.insert_into_collection(
                SURVEYS_COLLECTION, gdf, database=DATABASE, crs=4326
            )
            print(f"Successfully ingested {len(gdf)} surveys of artificial targets.")
        else:
            print("No new surveys ingested.")

    def read_art_surveys(self, calibration_site_xls: str) -> Optional[GeoDataFrame]:
        surveys_df = pd.read_excel(
            calibration_site_xls,
            "survey",
            skiprows=range(1, 5),
            converters={
                "Survey date (YYYY-MM-DD)": str,
                "GNSS measusement duration\n(hh:mm:ss)": str,
                "GNSS measurement duration\n(hh:mm:ss)": str,
            },
        )
        surveys_df.rename(
            columns={
                "Unique Target ID": "unique_target_id",
                "Survey date (YYYY-MM-DD)": "survey_date",
                "Latitude (decimal deg)": "lat",
                "Longitude (decimal deg)": "lon",
                "Elevation (m)": "elevation",
                "Position accuracy (cm)": "position_accuracy",
                "Coordinate Reference System (WKT or EPSG)": "coordinate_reference_system",
                "Azimuth angle\n(decimal deg)": "azimuth_angle",
                "Boresight angle\n(decimal deg)": "boresight_angle",
                "Tilt (decimal deg)": "tilt",
                "Pointing accuracy\n(decimal deg)": "accuracy",
                "Fence": "fence",
                "Measurement method": "measurement_method",
                "Offset method": "offset_method",
                "Applied corrections": "corrections",
                "GNSS measusement duration\n(hh:mm:ss)": "duration",
                "GNSS measurement duration\n(hh:mm:ss)": "duration",
                "Photo link": "photo_link",
                "Status report": "status_report",
            },
            inplace=True,
            errors="ignore",
        )

        surveys_df = surveys_df[surveys_df["unique_target_id"] != ""]
        surveys_df = surveys_df[surveys_df.unique_target_id.notnull()]
        surveys_df = surveys_df.replace(math.nan, None)
        surveys_df["elevation"] = surveys_df["elevation"].astype(float)
        surveys_df = surveys_df.dropna(axis=0, how="all")

        self.validate_art_surveys(surveys_df)
        if self.validation_mode:
            return None
        print("Surveys validation successful.")

        surveys_df.insert(len(surveys_df.columns), "geometry", "POINT(0 0)")
        surveys_df["geometry"] = gpd.GeoSeries.from_wkt(surveys_df["geometry"])
        return gpd.GeoDataFrame(surveys_df, geometry="geometry", crs=4326)

    def upload_photos(self, df: DataFrame):
        folder_id = self.get_folder_id("ext_pictures")

        for i, row in df.iterrows():
            if row["photo_link"]:
                photo_link = row["photo_link"]
                if photo_link.startswith("http"):
                    a = urllib.parse.urlparse(photo_link)
                    filename = os.path.basename(a.path)
                    while os.path.exists(filename):
                        identifier = "".join(
                            random.choices(string.ascii_lowercase, k=8)
                        )
                        parts = filename.split(".")
                        filename = f"{'.'.join(parts[:-1])}_{identifier}.{parts[-1]}"
                    photo_response = requests.get(photo_link)
                    with open(filename, "wb") as f:
                        f.write(photo_response.content)
                else:
                    filename = photo_link

                if not os.path.exists(filename):
                    print(f"WARN: Photo {filename} does not exist. Skipping.")
                    continue

                print(f"Uploading {filename}...")
                with open(filename, mode="rb") as photo_file:
                    contents = photo_file.read()

                upload_response = requests.post(
                    "https://www.sarcalnet.org/wp-json/wp/v2/media",
                    auth=HTTPBasicAuth("thomas", self.admin_password),
                    headers={
                        "Content-Disposition": f"attachment; filename={filename}",
                        "Content-Type": "multipart/form-data",
                    },
                    data=contents,
                )
                df.at[i, "photo_link"] = upload_response.json()["source_url"]

                if photo_link.startswith("http"):
                    os.remove(filename)
                if upload_response.status_code >= 300:
                    raise ValueError(upload_response.content)

                payload = {"folder": folder_id, "ids": upload_response.json()["id"]}
                move_response = requests.post(
                    f"https://www.sarcalnet.org/wp-json/filebird/public/v1/folder/set-attachment",
                    headers={"Authorization": f"Bearer {self.filebird_token}"},
                    json=payload,
                )

                if move_response.status_code >= 300:
                    raise ValueError(move_response.content)

    def validate_nat_surveys(self, surveys_df):
        mandatory_fields = [
            "unique_target_id",
            "survey_start",
            "survey_stop",
            "mission",
            "carrier_frequency",
            "polarization_channels",
            "observation_time_utc",
            "incidence_angle_range",
            "backscatter_coefficient_type",
            "backscatter_coefficient_mean",
            "backscatter_coefficient_std",
            "samples",
            "relative_orbit",
            "orbit_direction",
            "look_side",
            "acquisition_mode",
            "beam_id",
        ]
        for row in surveys_df.iterrows():
            for col in mandatory_fields:
                if str(row[1][col]) == "nan":
                    message = (
                        f"surveys, row {row[0] + 6}: missing entry for mandatory field "
                        f"'{col}'. Please fill in all mandatory "
                        f"fields, or remove the entire row."
                    )
                    if self.validation_mode:
                        print(message)
                    else:
                        raise ValueError(message)
            try:
                dateutil.parser.parse(row[1]["survey_start"])
            except dateutil.parser.ParserError:
                message = (
                    f"surveys, row {row[0] + 6}: invalid entry "
                    f"'{row[1]['survey_start']}' for mandatory field "
                    f"'Survey start (YYYY-MM-DD)'. Please enter a "
                    f"valid date."
                )
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)
            try:
                if row[1]["survey_stop"] != "-":
                    dateutil.parser.parse(row[1]["survey_stop"])
            except dateutil.parser.ParserError:
                message = (
                    f"surveys, row {row[0] + 6}: invalid entry "
                    f"'{row[1]['survey_stop']}' for mandatory field "
                    f"'Stop Monitoring Period (YYYY-MM-DD or \"-\")'. Please enter a "
                    f"valid date. "
                )
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)

    def validate_art_surveys(self, surveys_df):
        mandatory_fields = [
            "unique_target_id",
            "survey_date",
            "lat",
            "lon",
            "elevation",
            "position_accuracy",
            "coordinate_reference_system",
            "azimuth_angle",
            "boresight_angle",
            "tilt",
            "accuracy",
            "fence",
            "measurement_method",
            "offset_method",
        ]
        for row in surveys_df.iterrows():
            for col in mandatory_fields:
                if str(row[1][col]) == "nan":
                    message = (
                        f"surveys, row {row[0] + 6}: missing entry for "
                        f"mandatory field '{col}'. Please fill in all mandatory "
                        f"fields, or remove the entire row."
                    )
                    if self.validation_mode:
                        print(message)
                    else:
                        raise ValueError(message)
            try:
                dateutil.parser.parse(row[1]["survey_date"])
            except (dateutil.parser.ParserError, TypeError):
                message = (
                    f"surveys, row {row[0] + 6}: invalid entry "
                    f"'{row[1]['survey_date']}' for mandatory field "
                    f"'Survey date (YYYY-MM-DD)'. Please enter a valid date."
                )
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)

            self.validate_lat(row)
            self.validate_lon(row)
            self.validate_position_accuracy(row)
            self.validate_crs(row)
            self.validate_azimuth_angle(row)
            self.validate_boresight_angle(row)
            self.validate_tilt(row)
            self.validate_accuracy(row)
            self.validate_duration(row)

    def validate_lat(self, row):
        if row[1]["lat"] < -90 or row[1]["lat"] > 90:
            message = (
                f"surveys, row {row[0] + 6}: invalid entry '{row[1]['lat']}' "
                f"for mandatory field 'Latitude (decimal deg)'. Please enter a "
                f"valid floating point number > -90 and < 90."
            )
            if self.validation_mode:
                print(message)
            else:
                raise ValueError(message)

    def validate_lon(self, row):
        if row[1]["lon"] < -180 or row[1]["lon"] > 180:
            message = (
                f"surveys, row {row[0] + 6}: invalid entry '{row[1]['lon']}' "
                f"for mandatory field 'Longitude (decimal deg)'. Please enter a "
                f"valid floating point number > -180 and < 180."
            )
            if self.validation_mode:
                print(message)
            else:
                raise ValueError(message)

    def validate_position_accuracy(self, row):
        message = (
            f"surveys, row {row[0] + 6}: invalid entry "
            f"'{row[1]['position_accuracy']}' for mandatory field 'Position accuracy "
            f"(cm)'. Please enter a valid floating point number > 0."
        )
        try:
            float(row[1]["position_accuracy"])
        except ValueError:
            if self.validation_mode:
                print(message)
                return
            else:
                raise ValueError(message)
        if row[1]["position_accuracy"] <= 0:
            if self.validation_mode:
                print(message)
            else:
                raise ValueError(message)

    def validate_crs(self, row):
        crs = row[1]["coordinate_reference_system"]
        try:
            pyproj.CRS.from_user_input(crs)
        except pyproj.exceptions.CRSError:
            message = (
                f"surveys, row {row[0] + 6}: invalid entry '{crs}' for "
                f"mandatory field 'Coordinate Reference System (WKT or EPSG)'. "
                f"Please enter a valid WKT or EPSG (e.g. EPSG:4326)."
            )
            if self.validation_mode:
                print(message)
            else:
                raise ValueError(message)

    def validate_duration(self, row):
        try:
            duration_parts = row[1]["duration"].split(":")
            if len(duration_parts) != 3:
                raise ValueError
            int(duration_parts[0])
            int(duration_parts[1])
            int(duration_parts[2])
        except ValueError:
            message = (
                f"surveys, row {row[0] + 6}: invalid entry for field "
                f"'GNSS measurement duration (hh:mm:ss)'."
                f" Please provide a time expressed "
                f"in hh:mm:ss format."
            )
            if self.validation_mode:
                print(message)
            else:
                raise ValueError(message)

    def validate_tilt(self, row):
        message = (
            f"surveys, row {row[0] + 6}: invalid entry '{row[1]['tilt']}' "
            f"for mandatory field 'Tilt angle (decimal deg)'. Please enter a "
            f"valid floating point number >= 0 and < 360."
        )
        try:
            float(row[1]["tilt"])
        except ValueError:
            if self.validation_mode:
                print(message)
                return
            else:
                raise ValueError(message)
        if type(row[1]["tilt"]) == str or row[1]["tilt"] < 0 or row[1]["tilt"] >= 360:
            if self.validation_mode:
                print(message)
            else:
                raise ValueError(message)

    def validate_boresight_angle(self, row):
        message = (
            f"surveys, row {row[0] + 6}: invalid entry '{row[1]['boresight_angle']}' "
            f"for mandatory field 'Boresight angle (decimal deg)'. Please enter a "
            f"valid floating point number >= -360 and <= 360."
        )
        try:
            float(row[1]["boresight_angle"])
        except ValueError:
            if self.validation_mode:
                print(message)
                return
            else:
                raise ValueError(message)
        if row[1]["boresight_angle"] < -360 or row[1]["boresight_angle"] > 360:
            if self.validation_mode:
                print(message)
            else:
                raise ValueError(message)

    def validate_azimuth_angle(self, row):
        message = (
            f"surveys, row {row[0] + 6}: invalid entry '{row[1]['azimuth_angle']}' for "
            f"mandatory field 'Azimuth angle (decimal deg)'. Please enter a "
            f"valid floating point number >= 0 and < 360."
        )
        try:
            float(row[1]["azimuth_angle"])
        except ValueError:
            if self.validation_mode:
                print(message)
                return
            else:
                raise ValueError(message)
        if float(row[1]["azimuth_angle"]) < 0 or float(row[1]["azimuth_angle"]) >= 360:
            if self.validation_mode:
                print(message)
            else:
                raise ValueError(message)

    def validate_accuracy(self, row):
        message = (
            f"surveys, row {row[0] + 6}: invalid entry '{row[1]['accuracy']}' "
            f"for mandatory field 'Pointing accuracy (decimal deg)'. Please enter a "
            f"valid floating point number >= 0."
        )
        try:
            float(row[1]["accuracy"])
        except ValueError:
            if self.validation_mode:
                print(message)
                return
            else:
                raise ValueError(message)

        if row[1]["accuracy"] < 0:
            if self.validation_mode:
                print(message)
            else:
                raise ValueError(message)

    def upload_file(
        self,
        media_file: str,
        folder_id: str,
        media_type: str,
        target_name: Optional[str] = None,
    ) -> Optional[str]:

        if self.validation_mode:
            return None

        if not target_name:
            target_name = os.path.basename(media_file)

        with open(media_file, mode="rb") as file_handle:
            contents = file_handle.read()
        print(f"Uploading {media_type} file {media_file}...")

        upload_response = requests.post(
            "https://www.sarcalnet.org/wp-json/wp/v2/media",
            auth=HTTPBasicAuth("thomas", self.admin_password),
            headers={
                "Content-Disposition": f"attachment; filename={target_name}",
                "Content-Type": "multipart/form-data",
            },
            data=contents,
        )
        if upload_response.status_code >= 300:
            raise ValueError(
                f"Unable to upload {media_type} file, reason: "
                + str(upload_response.content)
            )

        payload = {"folder": folder_id, "ids": upload_response.json()["id"]}
        move_response = requests.post(
            f"https://www.sarcalnet.org/wp-json/filebird/public/v1/folder/set-attachment",
            headers={"Authorization": f"Bearer {self.filebird_token}"},
            json=payload,
        )

        if move_response.status_code >= 300:
            raise ValueError(
                f"Unable to move {media_type} file to correct folder, reason: "
                + str(move_response.content)
            )

        return upload_response.json()["source_url"]

    def get_folder_id(self, folder_name: str) -> str:
        folders_response = requests.get(
            f"https://www.sarcalnet.org/wp-json/filebird/public/v1/folders",
            headers={"Authorization": f"Bearer {self.filebird_token}"},
        )
        folder_id = None
        for f in folders_response.json()["data"]["folders"]:
            if f["text"] == folder_name:
                folder_id = f["id"]
        if not folder_id:
            raise ValueError("Folder {folder_name} does not exist.")
        return folder_id

    def upload_license(self, license_file: Optional[str] = None) -> Optional[str]:
        if self.validation_mode or not license_file:
            return None
        licenses_folder_id = self.get_folder_id("license_files")
        return self.upload_file(license_file, licenses_folder_id, "license")

    def upload_self_assessment_file(self, self_assessment_pdf: str):
        if self.validation_mode or not self_assessment_pdf:
            return None
        folder_id = self.get_folder_id("self_assessments")
        return self.upload_file(self_assessment_pdf, folder_id, "self assessment")

    def upload_form(self, form: str, site_ids: List[str]):
        if self.validation_mode:
            return None
        folder_id = self.get_folder_id("submission_forms")
        target_name = "_".join(site_ids) + "-" + os.path.basename(form)
        form_url = self.upload_file(form, folder_id, "submission form", target_name)
        for site_id in site_ids:
            self.geoDB.update_collection(
                SITES_COLLECTION,
                {"form_url": form_url},
                f"short_site_identifier=eq.{site_id}",
                database=DATABASE,
            )

    def compute_centroid_from_boundaries(self, row):
        if (isinstance(row["centroid"], str) and not row["centroid"]) or (
            isinstance(row["centroid"], float) and math.isnan(row["centroid"])
        ):
            polygon = shapely.from_wkt(row["boundaries"])
            row["centroid"] = shapely.centroid(polygon).wkt
            return row
        else:
            if re.search(r"\s*-?\d+.\d*\s*,\s*-?\d+.\d*\s*", str(row["centroid"])):
                lat = row["centroid"].split(",")[0].strip()
                lon = row["centroid"].split(",")[1].strip()
                row["centroid"] = f"POINT({lon} {lat})"
                return row
            elif str(row["centroid"]).upper().startswith("POINT"):
                return row
            else:
                message = (
                    f"site, row {row.name + 6}: Invalid value for field "
                    f"'centroid'."
                    f"Please either provide a position given by comma-separated "
                    f"lat and lon values (e.g. 36.578, 120.356), or provide the "
                    f"position as WKT (e.g. POINT(120.356 36.578)), or leave the "
                    f"field empty, so the centroid is computed from the boundaries."
                )
                if self.validation_mode:
                    print(message)
                else:
                    raise ValueError(message)

    def ingest_unavailabilities(self, unavailability_xls: str):
        unavailabilities = pd.read_excel(
            unavailability_xls,
            "unavailability",
            skiprows=range(1, 5),
            converters={
                "Start of Unavailability (YYYY-MM-DD)": str,
                "End of Unavailability (YYYY-MM-DD)": str,
            },
        )
        unavailabilities.rename(
            columns={
                "Unique Target ID": "unique_target_id",
                "Start of Unavailability (YYYY-MM-DD)": "unavailability_start",
                "End of Unavailability (YYYY-MM-DD)": "unavailability_end",
            },
            inplace=True,
        )

        unavailabilities = unavailabilities.groupby(
            "unique_target_id", as_index=False
        ).agg({"unavailability_start": list, "unavailability_end": list})

        form_url = self.upload_unavailability_xls(unavailability_xls)

        for index, row in unavailabilities.iterrows():
            unique_target_id = row["unique_target_id"]
            previous_values = self.geoDB.get_collection(
                TARGETS_COLLECTION,
                query=f"select=unavailability_start,"
                f"unavailability_end,unavailability_forms&unique_target_id=eq.{unique_target_id}",
                database=DATABASE,
            )
            new_unavailability_start = (
                (
                    previous_values["unavailability_start"][0]
                    + unavailabilities["unavailability_start"][0]
                )
                if previous_values["unavailability_start"][0]
                else unavailabilities["unavailability_start"][0]
            )
            new_unavailability_end = (
                (
                    previous_values["unavailability_end"][0]
                    + unavailabilities["unavailability_end"][0]
                )
                if previous_values["unavailability_end"][0]
                else unavailabilities["unavailability_end"][0]
            )
            new_unavailability_forms = (
                [form_url]
                if previous_values["unavailability_forms"][0] is None
                else previous_values["unavailability_forms"][0] + [form_url]
            )
            self.geoDB.update_collection(
                TARGETS_COLLECTION,
                values={
                    "unavailability_start": new_unavailability_start,
                    "unavailability_end": new_unavailability_end,
                    "unavailability_forms": new_unavailability_forms,
                },
                query=f"unique_target_id=eq.{unique_target_id}",
                database=DATABASE,
            )
            print(f"Updated unavailability periods for target {unique_target_id}")

    def upload_unavailability_xls(self, xls: str) -> str:
        folder_id = self.get_folder_id("submission_forms")
        target_name = os.path.basename(xls)
        return self.upload_file(xls, folder_id, "unavailability form", target_name)
