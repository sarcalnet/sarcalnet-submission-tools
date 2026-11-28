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
import os
import warnings

import click
import pyproj

from scn_db_tools.ingester import Ingester


@click.command()
@click.option(
    "--server_url",
    default="https://xcube-geodb.brockmann-consult.de",
    help="The geoDB server URL.",
)
@click.option("--server_port", default=443, help="The geoDB server port.")
@click.option("--client_id", required=False, help="The geoDB client_id.")
@click.option("--client_secret", required=False, help="The geoDB client_secret.")
@click.option("--admin_password", required=False, help="The WordPress admin password.")
@click.option(
    "--filebird_token",
    required=False,
    help="The REST API Key of the Wordpress FileBird plugin.",
)
@click.option(
    "--auth_domain",
    default="https://winchester.production.brockmann-consult.de/winchester",
    help="The geoDB auth domain URL.",
)
@click.option(
    "--proj_dir",
    help="The path to the pyproj directory, usually ${env}\\Library\\share\\proj. Please "
    "set if proj errors occur.",
)
@click.option("--license_file", help="The license information in a signed PDF file.")
@click.argument(
    "calibration_site_xls",
    metavar="CALIBRATION_SITES_FILE",
)
@click.argument(
    "self_assessment_pdf",
    metavar="SELF_ASSESSMENT_FILE",
)
def ingest_calibration_info(
    calibration_site_xls: str,
    self_assessment_pdf: str,
    server_url: str,
    server_port: int,
    client_id: str,
    client_secret: str,
    admin_password: str,
    filebird_token: str,
    proj_dir: str,
    auth_domain: str,
    license_file: str,
):
    """
    Ingests the calibration sites given in CALIBRATION_SITES_FILE into the database.
    Expects a self-assessment given as SELF_ASSESSMENT_FILE.

    See https://www.sarcalnet.org/submission-templates/ for directions and
    templates (only for registered users).


    If run without any of the parameters `client_id`, `client_secret`,
    `admin_password`, or `filebird_token`, the ingester is run in
    validation mode, i.e. the provided calibration sites file is validated
    and potential errors are reported. Note that not every kind of error
    may automatically be identified.
    """

    warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

    if proj_dir:
        os.environ["PROJ_DIR"] = proj_dir
        os.environ["PROJ_LIBDIR"] = proj_dir
        pyproj.datadir.set_data_dir(proj_dir)

    validate_self_assessment_file(self_assessment_pdf)
    validate_license_file(license_file)

    ingester = Ingester(
        server_url,
        server_port,
        client_id,
        client_secret,
        auth_domain,
        admin_password,
        filebird_token,
    )

    license_url = ingester.upload_license(license_file)
    site_ids = ingester.ingest_sites(calibration_site_xls, license_url)
    target_type = ingester.ingest_targets(calibration_site_xls)
    ingester.ingest_surveys(calibration_site_xls, target_type)
    ingester.upload_self_assessment_file(self_assessment_pdf)
    ingester.upload_form(calibration_site_xls, site_ids)


def validate_self_assessment_file(self_assessment_pdf: str):
    base_error_message = (
        "Please provide a valid self assessment file in PDF format, according to "
        "the template provided on https://www.sarcalnet.org/?page_id=1278."
    )
    if not self_assessment_pdf.lower().endswith("pdf"):
        raise ValueError(
            f"{base_error_message}\nThe path {self_assessment_pdf} is not a pdf file."
        )
    if not os.path.exists(self_assessment_pdf):
        raise ValueError(
            f"{base_error_message}\nThe path {self_assessment_pdf} does not exist."
        )


def validate_license_file(license_file: str):
    if license_file:
        if not license_file.lower().endswith("pdf"):
            raise ValueError(
                "Please provide a valid signed license file in PDF"
                "format, according to the directions provided on "
                "https://www.sarcalnet.org/?page_id=1050."
            )
        if not os.path.exists(license_file):
            raise ValueError(f"The path {license_file} does not exist.")


if __name__ == "__main__":
    ingest_calibration_info()
