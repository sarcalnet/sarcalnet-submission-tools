import io
import os
import pkgutil
import unittest
from typing import Optional, Union
from unittest.mock import patch

import pandas
from geopandas import GeoDataFrame
from pandas import DataFrame
from xcube_geodb.core.geodb import GeoDBClient

from scn_db_tools.fill_template_from_db import DataFetcher, Outputter


class TemplateFillerTest(unittest.TestCase):
    @patch("xcube_geodb.core.geodb.GeoDBClient")
    def test_data_fetcher(self, mockgc: GeoDBClient):
        def get_collection_pg(
            collection: str,
            database: str,
            where: Optional[str] = None,
        ) -> Union[GeoDataFrame, DataFrame, None]:
            if collection == "calibration_sites":
                if "OR" in where:
                    self.assertEqual(
                        "(short_site_id = 'VISB' AND primary_target_type_id = 'AT') OR (short_site_id = 'GNLD' AND primary_target_type_id = 'DT')",
                        where,
                    )
                    return self.read_csv("test_sites.csv")
                else:
                    self.assertEqual(
                        "(short_site_id = 'SOME' AND primary_target_type_id = 'DT')",
                        where,
                    )
                    return None
            elif collection == "calibration_targets":
                if "OR" in where:
                    self.assertEqual(
                        "site_name = 'Greenland' OR site_name = 'Visby'",
                        where,
                    )
                    return self.read_csv("test_targets.csv")
                else:
                    self.assertEqual(
                        "site_name = 'SOME'",
                        where,
                    )
                    return None
            elif collection == "calibration_surveys":
                if "OR" in where:
                    self.assertEqual(
                        "target_id = 'VISB-CR-0006' OR target_id = 'VISB-CR-0007'",
                        where,
                    )
                    return self.read_csv("test_surveys.csv")
                else:
                    self.assertEqual(
                        "site_name = 'SOME'",
                        where,
                    )
                    return None
            elif collection == "calibration_nat_targets":
                if "OR" in where:
                    self.assertEqual(
                        "site_name = 'Greenland' OR site_name = 'Visby'",
                        where,
                    )
                    return self.read_csv("test_nat_targets.csv")
                else:
                    self.assertEqual(
                        "site_name = 'SOME'",
                        where,
                    )
                    return None
            elif collection == "calibration_nat_surveys":
                if "OR" in where:
                    import re

                    m = re.match(
                        "target_id = 'GNLD-SI-0001' OR target_id = 'GNLD-SI-0002' .* OR target_id = 'GNLD-SI-0016'",
                        where,
                    )

                    self.assertIsNotNone(m)
                    return self.read_csv("test_nat_surveys.csv")
                else:
                    self.assertEqual(
                        "site_name = 'SOME'",
                        where,
                    )
                    return None
            else:
                raise ValueError(f"Unknown collection: {collection}")

        mockgc.get_collection_pg = get_collection_pg

        data = DataFetcher(mockgc).fetch_data(["SOME-DT"])
        self.assertDictEqual(
            {
                "sites": None,
                "targets": None,
                "surveys": None,
                "nat_targets": None,
                "nat_surveys": None,
            },
            data,
        )

        data = DataFetcher(mockgc).fetch_data(["VISB-AT", "GNLD-DT"])

        self.assertIsNotNone(data)
        self.assertEqual(5, len(data))

        ## verify site

        gnld_geometry = data["sites"]["geometry"][0]
        gnld_poc_email = data["sites"]["poc_email"][0]
        gnld_landcover = data["sites"]["landcover"][0]
        self.assertEqual(
            "POLYGON ((-40.31 79.44, -38.5 79.44, -38.5 73.36, -40.31 73.36, -40.31 79.44))",
            gnld_geometry,
        )
        self.assertEqual(gnld_poc_email, "casalvn@inta.es")
        self.assertEqual(gnld_landcover, "Dry Snow")

        visb_geometry = data["sites"]["geometry"][1]
        visb_poc_email = data["sites"]["poc_email"][1]
        visb_landcover = data["sites"]["landcover"][1]
        self.assertEqual(
            visb_geometry,
            "POLYGON ((18.36680591720137 57.65426152081254, 18.36688860787704 57.65386055246518, 18.36759536583763 57.65384231119398, 18.36764125972256 57.65424909736233, 18.36680591720137 57.65426152081254))",
        )
        self.assertEqual(visb_poc_email, "faramarz.nilfouroushan@lm.se")
        self.assertEqual(
            visb_landcover,
            "The site is located approximately  53m above MSL, adjacent to twin GNSS stations in a remote area. The terrain is flat  exposed bedrock, with small shrubs, and scattered small trees",
        )

        ## verify targets

        self.assertEqual(2, len(data["targets"]))
        visb_cr_0006 = data["targets"].iloc[[0]]
        self.assertEqual(
            "POINT (18.367144217 57.654021158)", visb_cr_0006["geometry"][0]
        )
        self.assertEqual("EAST", visb_cr_0006["primary_direction"][0])
        self.assertEqual(78.373, visb_cr_0006["approx_h"][0])

        visb_cr_0007 = data["targets"].iloc[[1]]
        self.assertEqual(
            "POINT (18.367136997 57.654015399)", visb_cr_0007["geometry"][1]
        )
        self.assertEqual("WEST", visb_cr_0007["primary_direction"][1])
        self.assertEqual(78.362, visb_cr_0007["approx_h"][1])

        ## verify nat. targets

        self.assertEqual(16, len(data["nat_targets"]))
        gnld_si_0001 = data["nat_targets"].iloc[[0]]
        self.assertEqual(
            "POLYGON ((-39.2962 79.1496, -39.2695 79.1463, -39.2502 79.154, -39.2804 79.1563, -39.2962 79.1496))",
            gnld_si_0001["geometry"][0],
        )
        self.assertEqual(0.59, gnld_si_0001["coverage"][0])
        self.assertEqual("79.1496N_39.2962W_drySnow", gnld_si_0001["internal_id"][0])

        gnld_si_0016 = data["nat_targets"].iloc[[15]]
        self.assertEqual(
            "POLYGON ((-39.8741 73.6711, -39.858 73.674, -39.8681 73.6782, -39.8862 73.6754, -39.8741 73.6711))",
            gnld_si_0016["geometry"][15],
        )
        self.assertEqual(0.36, gnld_si_0016["coverage"][15])
        self.assertEqual("73.6711N_39.8741W_drySnow", gnld_si_0016["internal_id"][15])

        ## verify surveys

        self.assertEqual(2, len(data["surveys"]))
        survey_1 = data["surveys"].iloc[[0]]
        self.assertEqual(
            5,
            survey_1["position_accuracy"][0],
        )
        self.assertEqual(100, survey_1["azimuth_angle"][0])
        self.assertEqual(54.74, survey_1["boresight_angle"][0])

        survey_2 = data["surveys"].iloc[[1]]
        self.assertEqual(
            5,
            survey_2["position_accuracy"][1],
        )
        self.assertEqual(260, survey_2["azimuth_angle"][1])
        self.assertEqual(54.74, survey_2["boresight_angle"][1])

        ## verify nat. surveys

        self.assertEqual(16, len(data["nat_surveys"]))
        nat_survey_1 = data["nat_surveys"].iloc[[0]]
        self.assertEqual("HH", nat_survey_1["polarizations"][0])
        self.assertEqual("35,07 - 36.47", nat_survey_1["incidence_angle_range"][0])
        self.assertEqual(-4.39, nat_survey_1["backscatter_coeff_mean"][0])

        nat_survey_16 = data["nat_surveys"].iloc[[15]]
        self.assertEqual("VV", nat_survey_16["polarizations"][15])
        self.assertEqual("29,66 - 31,21", nat_survey_16["incidence_angle_range"][15])
        self.assertEqual(-7.49, nat_survey_16["backscatter_coeff_mean"][15])

    @patch("xcube_geodb.core.geodb.GeoDBClient")
    def test_outputter(self, mockgc: GeoDBClient):
        def get_collection_pg(
            collection: str,
            database: str,
            where: Optional[str] = None,
        ) -> Union[GeoDataFrame, DataFrame, None]:
            if collection == "calibration_sites":
                return self.read_csv("test_sites.csv")
            elif collection == "calibration_targets":
                return self.read_csv("test_targets.csv")
            elif collection == "calibration_surveys":
                return self.read_csv("test_surveys.csv")
            elif collection == "calibration_nat_targets":
                return self.read_csv("test_nat_targets.csv")
            elif collection == "calibration_nat_surveys":
                return self.read_csv("test_nat_surveys.csv")
            else:
                raise ValueError(f"Unknown collection: {collection}")

        mockgc.get_collection_pg = get_collection_pg
        is_ci_run = bool(os.getenv("GITHUB_ACTIONS", False))
        if is_ci_run:
            tempdir = os.path.join(os.environ["RUNNER_TEMP"], "processing")
        else:
            tempdir = "./scn_db_tools/test/res"
        temp_file = os.path.join(tempdir, os.urandom(10).hex() + ".xlsx")
        data = DataFetcher(mockgc).fetch_data(["VISB-AT", "GNLD-DT"])
        try:
            Outputter().write_form(
                data,
                temp_file,
            )
            sites = pandas.read_excel(temp_file, "site")
            self.assertEqual(
                "Greenland",
                sites["Site Name"][0],
            )
            self.assertEqual(
                "Visby",
                sites["Site Name"][1],
            )
            cr = pandas.read_excel(temp_file, "cr")
            self.assertEqual(36.8, cr["Reference RCS (dBm2)"][0])
            self.assertEqual(34.2, cr["Reference RCS (dBm2)"][1])
            dt = pandas.read_excel(temp_file, "dt")
            self.assertEqual("79.1496N_39.2962W_drySnow", dt["Internal ID"][0])
            self.assertEqual("73.6711N_39.8741W_drySnow", dt["Internal ID"][15])
            surveys = pandas.read_excel(temp_file, "surveys")
            self.assertEqual(53.453, surveys["Elevation (m)"][0])
            self.assertEqual(53.4422, surveys["Elevation (m)"][1])
        finally:
            os.remove(temp_file)

    @staticmethod
    def read_csv(csv) -> DataFrame:
        data = pkgutil.get_data("scn_db_tools.test.res", csv).decode()
        df = pandas.read_csv(io.StringIO(data))
        df.drop(columns=["Unnamed: 0", "unknown"], inplace=True, errors="ignore")
        return df
