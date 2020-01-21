import os
from dataclasses import dataclass
from typing import Union, Iterable


RS_DELTA_MAGIC = 0x72730236

(
    RS_KIND_END,
    RS_KIND_LITERAL,
    RS_KIND_SIGNATURE,
    RS_KIND_COPY,
    RS_KIND_CHECKSUM,
    RS_KIND_RESERVED,
    RS_KIND_INVALID
) = range(1000, 1000 + 7)


rs_kinds = [
    "RS_KIND_END",
    "RS_KIND_LITERAL",
    "RS_KIND_SIGNATURE",
    "RS_KIND_COPY",
    "RS_KIND_CHECKSUM",
    "RS_KIND_RESERVED",
    "RS_KIND_INVALID"
]

"""
typedef struct rs_prototab_ent {
    enum rs_op_kind kind;
    int immediate;
    size_t len_1, len_2;
} rs_prototab_ent_t;
"""


class rs_prototab_ent:
    def __init__(self, kind, immediate, len_1, len_2):
        self.kind = kind
        self.immediate = immediate
        self.len_1 = len_1
        self.len_2 = len_2

    def __repr__(self):
        return "({}, {}, {}, {})".format(
            rs_kinds[self.kind - 1000], self.immediate, self.len_1, self.len_2
        )


rs_prototab = [
    rs_prototab_ent(RS_KIND_END, 0, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 1, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 2, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 3, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 4, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 5, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 6, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 7, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 8, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 9, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 10, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 11, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 12, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 13, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 14, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 15, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 16, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 17, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 18, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 19, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 20, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 21, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 22, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 23, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 24, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 25, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 26, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 27, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 28, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 29, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 30, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 31, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 32, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 33, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 34, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 35, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 36, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 37, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 38, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 39, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 40, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 41, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 42, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 43, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 44, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 45, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 46, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 47, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 48, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 49, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 50, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 51, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 52, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 53, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 54, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 55, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 56, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 57, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 58, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 59, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 60, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 61, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 62, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 63, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 64, 0, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 0, 1, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 0, 2, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 0, 4, 0),
    rs_prototab_ent(RS_KIND_LITERAL, 0, 8, 0),
    rs_prototab_ent(RS_KIND_COPY, 0, 1, 1),
    rs_prototab_ent(RS_KIND_COPY, 0, 1, 2),
    rs_prototab_ent(RS_KIND_COPY, 0, 1, 4),
    rs_prototab_ent(RS_KIND_COPY, 0, 1, 8),
    rs_prototab_ent(RS_KIND_COPY, 0, 2, 1),
    rs_prototab_ent(RS_KIND_COPY, 0, 2, 2),
    rs_prototab_ent(RS_KIND_COPY, 0, 2, 4),
    rs_prototab_ent(RS_KIND_COPY, 0, 2, 8),
    rs_prototab_ent(RS_KIND_COPY, 0, 4, 1),
    rs_prototab_ent(RS_KIND_COPY, 0, 4, 2),
    rs_prototab_ent(RS_KIND_COPY, 0, 4, 4),
    rs_prototab_ent(RS_KIND_COPY, 0, 4, 8),
    rs_prototab_ent(RS_KIND_COPY, 0, 8, 1),
    rs_prototab_ent(RS_KIND_COPY, 0, 8, 2),
    rs_prototab_ent(RS_KIND_COPY, 0, 8, 4),
    rs_prototab_ent(RS_KIND_COPY, 0, 8, 8),
    rs_prototab_ent(RS_KIND_RESERVED, 85, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 86, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 87, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 88, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 89, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 90, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 91, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 92, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 93, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 94, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 95, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 96, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 97, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 98, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 99, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 100, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 101, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 102, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 103, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 104, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 105, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 106, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 107, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 108, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 109, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 110, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 111, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 112, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 113, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 114, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 115, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 116, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 117, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 118, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 119, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 120, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 121, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 122, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 123, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 124, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 125, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 126, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 127, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 128, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 129, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 130, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 131, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 132, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 133, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 134, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 135, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 136, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 137, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 138, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 139, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 140, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 141, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 142, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 143, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 144, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 145, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 146, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 147, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 148, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 149, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 150, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 151, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 152, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 153, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 154, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 155, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 156, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 157, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 158, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 159, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 160, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 161, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 162, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 163, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 164, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 165, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 166, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 167, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 168, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 169, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 170, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 171, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 172, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 173, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 174, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 175, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 176, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 177, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 178, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 179, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 180, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 181, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 182, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 183, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 184, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 185, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 186, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 187, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 188, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 189, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 190, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 191, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 192, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 193, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 194, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 195, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 196, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 197, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 198, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 199, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 200, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 201, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 202, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 203, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 204, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 205, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 206, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 207, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 208, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 209, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 210, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 211, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 212, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 213, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 214, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 215, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 216, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 217, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 218, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 219, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 220, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 221, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 222, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 223, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 224, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 225, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 226, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 227, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 228, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 229, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 230, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 231, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 232, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 233, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 234, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 235, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 236, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 237, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 238, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 239, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 240, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 241, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 242, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 243, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 244, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 245, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 246, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 247, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 248, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 249, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 250, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 251, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 252, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 253, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 254, 0, 0),
    rs_prototab_ent(RS_KIND_RESERVED, 255, 0, 0),
]


class Cursor:
    def __init__(self, fd):
        self.fd = fd
        fd.seek(0, os.SEEK_SET)

    def read_byte(self):
        return self.fd.read(1)[0]

    def read_netint(self, length):
        assert length <= 8
        data = self.fd.read(length)
        result = 0
        for i in range(length):
            result = (result << 8) | data[i]
        return result

    def read_buffer(self, length):
        return self.fd.read(length)

    def skip(self, length):
        self.fd.seek(length, os.SEEK_CUR)


@dataclass
class LiteralDeltaCommand:
    length: int


@dataclass
class CopyDeltaCommand:
    start: int
    length: int


DeltaCommand = Union[LiteralDeltaCommand, CopyDeltaCommand]


def handle_literal(cmd: rs_prototab_ent, cursor: Cursor) -> DeltaCommand:
    if cmd.len_1:
        length = cursor.read_netint(cmd.len_1)
    else:
        length = cmd.immediate

    # param2 is not used

    cursor.skip(length)
    return LiteralDeltaCommand(length=length)


def handle_copy(cmd: rs_prototab_ent, cursor: Cursor) -> DeltaCommand:
    start = cursor.read_netint(cmd.len_1)
    length = cursor.read_netint(cmd.len_2)

    return CopyDeltaCommand(start=start, length=length)


def handle_unsupported(*args):
    raise Exception("Unsupported command", *args)


command_handlers = {
    RS_KIND_LITERAL: handle_literal,
    RS_KIND_COPY: handle_copy
}


def parse_delta(cursor: Cursor) -> Iterable[DeltaCommand]:
    magic = cursor.read_netint(4)
    if magic != RS_DELTA_MAGIC:
        raise Exception("Invalid magic number", magic)

    cmd = rs_prototab[cursor.read_byte()]
    while cmd.kind != RS_KIND_END:
        handler = command_handlers.get(cmd.kind, handle_unsupported)
        yield handler(cmd, cursor)
        cmd = rs_prototab[cursor.read_byte()]


def parse_delta_from_file(filename: str):
    with open(filename, "rb") as fd:
        yield from parse_delta(Cursor(fd))
