# SPDX-License-Identifier: MIT
#
# Copyright (c) 2025, the xcube team
#               2026, Antonio Valentino
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

# /// script
# dependencies = [
#   "openpyxl",
#   "argcomplete",
# ]
# requires-python = ">=3.11"
# ///

"""Generate documentation for the SARCalNet submission template.

The tool analyzes the SARCalNet submission template, extracts
the documentation for each of the fields, and generate
ReStructuredText documentation.

The input template file in XLSX format is assumed to have:
* display title in row 1,
* field name in row 2,
* indication of mandatory/optional fields in row 3,
* literal comment blocks in rows 4-5.

"""

import abc
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
        self,
        filename,
        skip_sheet: Collection[str] = DEFAULT_SKIP_SHEETS,
        field_info_rows: FieldInfoRows = DEFAULT_INFO_ROWS,
    ):
        self._wb = openpyxl.load_workbook(
            filename,
            read_only=True,
            data_only=False,
        )
        self._skip_sheets = {sheet.casefold() for sheet in skip_sheet}
        self._field_info_rows = FieldInfoRows(*field_info_rows)

    def iter_sections(self) -> Iterator[str]:
        for title in self._wb.sheetnames:
            if title.casefold() not in self._skip_sheets:
                yield title

    @functools.cache  # noqa: B019
    def _used_columns(self, section: str, rows: FieldInfoRows) -> list[int]:
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
        self, section: str, info_rows: FieldInfoRows | None = None,
    ) -> Iterator[FieldInfo]:
        info_rows: FieldInfoRows = (
            info_rows if info_rows is not None else self._field_info_rows
        )

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


class BaseFormatter(abc.ABC):
    def __init__(
        self,
        model: DataModel,
        title: str,
        preamble: Collection[str] | None = None,
    ):
        self.model = model
        self.title = title
        self.preamble: list[str] = preamble if preamble is not None else []

    @staticmethod
    def make_title(title, char) -> list[str]:
        return [
            title,
            char * max(len(title), 3),            # "",
        ]

    @staticmethod
    def literal_block(text):
        lines = []

        if not text:
            return lines
        lines.append("::")
        lines.append("")
        for raw_line in text.splitlines():
            lines.append(f"   {raw_line}" if raw_line else "   ")

        return lines

    @staticmethod
    def field_block(label, value):
        lines = []
        if value:
            lines.append(f":{label}: {value}")
        return lines

    @abc.abstractmethod
    def format_doc(self) -> list[str]:
        pass

    def generate_doc(self) -> str:
        lines = self.format_doc()
        data = "\n".join(lines).rstrip() + "\n"
        return data


class SinglefileFormatter(BaseFormatter):
    def format_field(self, field_info: FieldInfo) -> list[str]:
        lines = []

        column_title = (field_info.title or field_info.field_id)
        lines.extend(self.make_title(column_title, "~"))
        lines.extend(self.field_block("Field name", field_info.field_id))
        lines.extend(self.field_block("Required", field_info.mandatory))

        if field_info.description:
            lines.append("")
            lines.extend(self.make_title("Description", "^"))
            lines.append("")
            lines.extend(self.literal_block(field_info.description))

        if field_info.validity_range:
            lines.append("")
            lines.extend(self.make_title("Constraints", "^"))
            lines.append("")
            lines.extend(self.literal_block(field_info.validity_range))

        return lines

    def format_section(self, section: str) -> list[str]:
        lines = []

        lines.extend(self.make_title(section, "-"))
        lines.append("")

        for field_info in self.model.iter_fields(section):
            lines.extend(self.format_field(field_info))
            lines.append("")

        return lines

    def format_doc(self) -> list[str]:
        lines = []
        lines.extend(self.make_title(self.title, "="))
        if self.preamble:
            lines.append("")
            lines.extend(self.preamble)
            lines.append("")

        for section in self.model.iter_sections():
            section_lines = self.format_section(section)
            lines.extend(section_lines)

        return lines


def title_from_source(source: Path):
    stem = source.stem
    stem = re.sub(r"_v\d+(?:\.\d+)*$", "", stem)
    stem = stem.replace("_submission_template", " Submission Template")
    words = stem.replace("_", " ").replace("-", " ").split()
    return " ".join(
        "SARCalNet" if word.lower() == "sarcalnet" else word.title()
        for word in words
    )


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
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

    model = DataModel(
        source, skip_sheet=skip_sheets, field_info_rows=field_info_rows
    )

    preamble = [f"Source file: ``{source.name}``"]
    formatter = SinglefileFormatter(model, doc_title, preamble=preamble)
    data = formatter.generate_doc()

    output.write_text(data, encoding="utf-8")
    print(output.resolve())


if __name__ == "__main__":
    main()
