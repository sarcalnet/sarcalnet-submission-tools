## SARCalNet submission tools

This python package contains the SARCalNet submission tools. The tools can run 
in two modes:
1) validation mode. Basically, if no credentials to the SARCalNet 
database are provided, the software validates the input calibration
sites template, and exists. See below for directions on how to 
run in validation mode.
2) ingestion modes. If the credentials to the SARCalNet database 
are provided, the software validates the input calibration sites
template, and in case it can successfully be validated, it 
stores its contents in the SARCalNet database.

The software is run with the command `python ingester.py`. Running this 
command with the parameter `--help` prints its usage:

Usage: 

`ingester.py [OPTIONS] CALIBRATION_SITES_FILE SELF_ASSESSMENT_FILE`

Ingests the calibration sites given in `CALIBRATION_SITES_FILE` into the
database. Expects a self-assessment given as `SELF_ASSESSMENT_FILE`.

See https://www.sarcalnet.org/submission-templates/ for directions and 
templates (only for registered users).

If run without any of the parameters `client_id`, `client_secret`,
`admin_password`, or `filebird_token`, the ingester is run in validation
mode, i.e. the provided calibration sites file is validated and potential
errors are reported. Note that not every kind of error may automatically be
identified.

Options:

`--server_url TEXT`      The geoDB server URL.

`--server_port INTEGER`  The geoDB server port.

`--client_id TEXT`       The geoDB client_id.

`--client_secret TEXT`   The geoDB client_secret.

`--admin_password TEXT`  The WordPress admin password.

`--filebird_token TEXT`  The REST API Key of the Wordpress FileBird plugin.

`--auth_audience TEXT`   The geoDB auth audience URL.

`--proj_dir TEXT`        The path to the pyproj directory, usually
`${env}\Library\share\proj`. Please set if proj errors occur.

`--license_file TEXT`    The license information in a signed PDF file.

`--help`                 Show this message and exit.
