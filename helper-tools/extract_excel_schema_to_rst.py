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

# python extract_excel_schema_to_rst.py # prints usage
# python extract_excel_schema_to_rst.py /path/to/template.xlsx


import argparse
import functools
import re
from collections.abc import Collection, Iterator
from pathlib import Path
from typing import NamedTuple

import openpyxl


class FieldInfoRows(NamedTuple):
    title: int = 1
    field_id: int = 2
    mandatory: int = 3
    description: int = 4
    validity_range: int = 5


class FieldInfo(NamedTuple):
    title: str
    field_id: str
    mandatory: str
    description: str
    validity_range: str


DEFAULT_SKIP_SHEETS = frozenset({"README", "definitions"})
DEFAULT_INFO_ROWS = FieldInfoRows()


class DataModel:
    def __init__(
        self, filename, skip_sheet: Collection[str] = DEFAULT_SKIP_SHEETS
    ):
        self._wb = openpyxl.load_workbook(
            filename,
            read_only=True,
            data_only=False,
        )
        self._skip_sheets = {sheet.casefold() for sheet in skip_sheet}

    def iter_sections(self) -> Iterator[str]:
        for title in self._wb.sheetnames:
            if title.casefold() not in self._skip_sheets:
                yield title

    @functools.cache  # noqa: B019
    def _used_columns(self, section: str, rows: FieldInfoRows):
        ws = self._wb[section]
        columns = []
        for col in range(1, ws.max_column + 1):
            values = [ws.cell(row=row, column=col).value for row in rows]
            if any(value is not None for value in values):
                columns.append(col)
        return columns

    @staticmethod
    def _cell_text(value):
        return "" if value is None else str(value)

    def iter_fields(
        self, section: str, info_rows: FieldInfoRows = DEFAULT_INFO_ROWS,
    ) -> Iterator[FieldInfo]:
        ws = self._wb[section]
        for col in self._used_columns(section, info_rows):
            yield FieldInfo(
                title=self._cell_text(
                    ws.cell(row=info_rows.title, column=col).value
                ),
                field_id=self._cell_text(
                    ws.cell(row=info_rows.field_id, column=col).value
                ),
                mandatory=self._cell_text(
                    ws.cell(row=info_rows.mandatory, column=col).value
                ),
                description=self._cell_text(
                    ws.cell(row=info_rows.description, column=col).value
                ),
                validity_range=self._cell_text(
                    ws.cell(row=info_rows.validity_range, column=col).value
                ),
            )


def underline(title, char):
    return char * max(len(title), 3)


def literal_block(lines, text):
    if not text:
        return
    lines.append("::")
    lines.append("")
    for raw_line in text.splitlines():
        lines.append(f"   {raw_line}" if raw_line else "   ")


def field_block(lines, label, value):
    if value:
        lines.append(f":{label}: {value}")


def title_from_source(source):
    stem = source.stem
    stem = re.sub(r"_v\d+(?:\.\d+)*$", "", stem)
    stem = stem.replace("_submission_template", " Submission Template")
    words = stem.replace("_", " ").replace("-", " ").split()
    return " ".join("SARCalNet" if word.lower() == "sarcalnet" else word.title() for word in words)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Generate an RST schema summary from an Excel workbook. By default, "
            "row 1 is used as the display title, row 2 as the field name, row 3 "
            "as the status, and rows 4-5 as literal comment blocks."
        )
    )
    parser.add_argument("source", type=Path, help="Input .xlsx workbook")
    parser.add_argument("--output", type=Path, help="Output .rst file. Defaults to <source-stem>.rst")
    parser.add_argument("--title", help="Document title. Defaults to a readable title from the source file name")
    parser.add_argument(
        "--skip-sheet",
        action="append",
        default=list(DEFAULT_SKIP_SHEETS),
        help="Sheet name to skip. Can be repeated. Defaults to README and definitions",
    )
    parser.add_argument(
        "--title-row",
        type=int,
        default=DEFAULT_INFO_ROWS.title,
        help="Row containing human-readable column titles",
    )
    parser.add_argument(
        "--field-id-row",
        type=int,
        default=DEFAULT_INFO_ROWS.field_id,
        help="Row containing field IDs",
    )
    parser.add_argument(
        "--mandatory-row",
        type=int,
        default=DEFAULT_INFO_ROWS.mandatory,
        help="Row in which it is specified if a field is mandatory or not",
    )
    parser.add_argument(
        "--description-row",
        type=int,
        default=DEFAULT_INFO_ROWS.description,
        help="Row containing the description text",
    )
    parser.add_argument(
        "--validity-range-row",
        type=int,
        default=DEFAULT_INFO_ROWS.validity_range,
        help="Row containing the definition of validity ranges",
    )
    try:
        import argcomplete

        argcomplete.autocomplete(parser)
    except ImportError:
        pass
    return parser.parse_args()


def main():
    args = parse_args()
    source = args.source
    output = args.output or Path(f"{source.stem}.rst")
    doc_title = args.title or title_from_source(source)
    skip_sheets = {sheet.casefold() for sheet in args.skip_sheet}
    field_info_rows = FieldInfoRows(
        title=args.title_row,
        field_id=args.field_id_row,
        mandatory=args.mandatory_row,
        description=args.description_row,
        validity_range=args.validity_range_row,
    )

    model = DataModel(source, skip_sheet=skip_sheets)

    lines = [
        doc_title,
        underline(doc_title, "="),
        "",
        f"Source file: ``{source.name}``",
        "",
    ]

    for section in model.iter_sections():
        lines.extend([section, underline(section, "-"), ""])

        for field_info in model.iter_fields(section, info_rows=field_info_rows):
            column_title = (field_info.title or field_info.field_id)
            lines.extend([column_title, underline(column_title, "~")])
            field_block(lines, "Field name", field_info.field_id)
            field_block(lines, "Required", field_info.mandatory)

            if field_info.description:
                lines.extend(["", "Description", "^^^^^^^^^^^", ""])
                literal_block(lines, field_info.description)

            if field_info.validity_range:
                lines.extend(["", "Constraints", "^^^^^^^^^^^^^^^^^^^^^^", ""])
                literal_block(lines, field_info.validity_range)

            lines.append("")

    output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    print(output.resolve())


if __name__ == "__main__":
    main()
