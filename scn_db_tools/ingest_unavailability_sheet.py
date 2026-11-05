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
@click.option("--client_id", required=True, help="The geoDB client_id.")
@click.option("--client_secret", required=True, help="The geoDB client_secret.")
@click.option(
    "--auth_domain",
    default="https://winchester.production.brockmann-consult.de/winchester",
    help="The geoDB auth domain URL.",
)
@click.option("--admin_password", required=False, help="The WordPress admin password.")
@click.option(
    "--filebird_token",
    required=True,
    help="The REST API Key of the Wordpress FileBird plugin.",
)
@click.argument(
    "unavailability_xls",
    metavar="UNAVAILABILITY_FILE",
)
def ingest_unavailability_info(
    unavailability_xls: str,
    server_url: str,
    server_port: int,
    client_id: str,
    client_secret: str,
    auth_domain: str,
    admin_password: str,
    filebird_token: str,
):
    """
    Ingests the unavailability dates given in UNAVAILABILITY_FILE into the database.
    """

    warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

    ingester = Ingester(
        server_url,
        server_port,
        client_id,
        client_secret,
        auth_domain,
        admin_password,
        filebird_token,
    )

    ingester.ingest_unavailabilities(unavailability_xls)


if __name__ == "__main__":
    ingest_unavailability_info()
