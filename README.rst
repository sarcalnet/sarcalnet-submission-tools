SARCalNet Submission Tools
==========================

This repository hosts support tools used to manage ingestion of
calibration data into teh SARCalNet DataBase.

Content overview:

:api_demo:
    contains demo notebooks for the use of the SARCalNet Python API.
:helper-tools:
    contains helper tools.
    One of the helper tool can be used to extract the documentation of
    teh fields of submission template and produce a ReStructuredText
    document.
:scn_db_tools:
    contains tools to check the content of XLSX files containing the
    calibration data provided by the user, and ingest them in the
    SARCalNet DB.

:``submission-responses.md``:
    contains templates for the automated e-mail responses.


Contributing
------------

This project uses the Git `pre-commit <https://pre-commit.com/>`_ checks
to ensure that the code is aligned with basic coding standards requirements
and project rules.

Contributors are strongly encouraged to enable pre-commit just after cloning
Git repository::

    $ git clone git@github.com:sarcalnet/sarcalnet-submission-tools.git
    $ pre-commit install

Once pre commit is enabled, check are performed automatically at each commit.


License
-------


Copyright 2024-2026 the xcube team and the SARCalNet Team

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.