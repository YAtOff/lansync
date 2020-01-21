import ctypes
import ctypes.util
from contextlib import contextmanager
import os
import sys
import tempfile
from functools import wraps
from pathlib import Path

from librsync.util import force_bytes, resource_manager

try:
    import syslog
except ImportError:

    class syslog:
        LOG_EMERG = 0
        LOG_ALERT = 1
        LOG_CRIT = 2
        LOG_ERR = 3
        LOG_WARNING = 4
        LOG_NOTICE = 5
        LOG_INFO = 6
        LOG_DEBUG = 7


base_lib_path = Path(__file__).parent
lib_filename = (
    "librsync.so"
    if os.name == "posix"
    else "rsync.dll"
    if sys.platform == "win32"
    else None
)
if lib_filename is None:
    raise NotImplementedError("Librsync is not supported on your platform")
lib_path = os.fspath(base_lib_path / lib_filename)

try:
    _librsync = ctypes.cdll.LoadLibrary(lib_path)
except OSError:
    raise ImportError('Could not load librsync at "%s"' % lib_path)


MAX_SPOOL = 1024 ** 2 * 5

TRACE_LEVELS = (
    syslog.LOG_EMERG,
    syslog.LOG_ALERT,
    syslog.LOG_CRIT,
    syslog.LOG_ERR,
    syslog.LOG_WARNING,
    syslog.LOG_NOTICE,
    syslog.LOG_INFO,
    syslog.LOG_DEBUG,
)

RS_DONE = 0
RS_BLOCKED = 1

RS_JOB_BLOCKSIZE = 65536
RS_DEFAULT_STRONG_LEN = 8
RS_DEFAULT_BLOCK_LEN = 2048
RS_MD4_SIG_MAGIC = 0x72730136


CharPtr = ctypes.POINTER(ctypes.c_char)


def char_ptr_from_bytes(data):
    return ctypes.cast(data, CharPtr)


# DEFINES FROM librsync.h:
# -------------------------

# librsync.h: rs_buffers_s
class Buffer(ctypes.Structure):
    _fields_ = [
        ("next_in", CharPtr),
        ("avail_in", ctypes.c_size_t),
        ("eof_in", ctypes.c_int),
        ("next_out", CharPtr),
        ("avail_out", ctypes.c_size_t),
    ]


# char const *rs_strerror(rs_result r);
_librsync.rs_strerror.restype = ctypes.c_char_p
_librsync.rs_strerror.argtypes = (ctypes.c_int,)

# rs_job_t *rs_sig_begin(size_t new_block_len, size_t strong_sum_len);
_librsync.rs_sig_begin.restype = ctypes.c_void_p
_librsync.rs_sig_begin.argtypes = (
    ctypes.c_size_t,
    ctypes.c_size_t,
)

# rs_job_t *rs_loadsig_begin(rs_signature_t **);
_librsync.rs_loadsig_begin.restype = ctypes.c_void_p
_librsync.rs_loadsig_begin.argtypes = (ctypes.c_void_p,)

# rs_job_t *rs_delta_begin(rs_signature_t *);
_librsync.rs_delta_begin.restype = ctypes.c_void_p
_librsync.rs_delta_begin.argtypes = (ctypes.c_void_p,)

# rs_job_t *rs_patch_begin(rs_copy_cb *, void *copy_arg);
_librsync.rs_patch_begin.restype = ctypes.c_void_p
_librsync.rs_patch_begin.argtypes = (
    ctypes.c_void_p,
    ctypes.c_void_p,
)

# rs_result rs_build_hash_table(rs_signature_t* sums);
_librsync.rs_build_hash_table.restype = ctypes.c_size_t
_librsync.rs_build_hash_table.argtypes = (ctypes.c_void_p,)

# rs_result rs_job_iter(rs_job_t *, rs_buffers_t *);
_librsync.rs_job_iter.restype = ctypes.c_int
_librsync.rs_job_iter.argtypes = (
    ctypes.c_void_p,
    ctypes.c_void_p,
)

# void rs_trace_set_level(rs_loglevel level);
_librsync.rs_trace_set_level.restype = None
_librsync.rs_trace_set_level.argtypes = (ctypes.c_int,)

# void rs_free_sumset(rs_signature_t *);
_librsync.rs_free_sumset.restype = None
_librsync.rs_free_sumset.argtypes = (ctypes.c_void_p,)

# rs_result rs_job_free(rs_job_t *);
_librsync.rs_job_free.restype = ctypes.c_int
_librsync.rs_job_free.argtypes = (ctypes.c_void_p,)

# A function declaration for our read callback.
patch_callback = ctypes.CFUNCTYPE(
    ctypes.c_int,
    ctypes.c_void_p,
    # TODO: we need platform independant intmax_t
    ctypes.c_longlong if sys.platform == "win32" else ctypes.c_int,
    ctypes.POINTER(ctypes.c_size_t),
    ctypes.POINTER(ctypes.c_void_p),
)


FILE = ctypes.c_void_p


"""
/** Open a file with special handling for stdin or stdout.
 *
 * This provides a platform independent way to open large binary files. A
 * filename "" or "-" means use stdin for reading, or stdout for writing.
 *
 * \\param filename - The filename to open.
 *
 * \\param mode - fopen style mode string.
 *
 * \\param force - bool to force overwriting of existing files. */
LIBRSYNC_EXPORT FILE *rs_file_open(char const *filename, char const *mode,
                                   int force);
"""
_librsync.rs_file_open.restype = FILE
_librsync.rs_file_open.argtypes = (ctypes.c_char_p, ctypes.POINTER(ctypes.c_char), ctypes.c_int)


def fopen(path: str, flags: str) -> FILE:
    return _librsync.rs_file_open(force_bytes(path), force_bytes(flags), 1)


"""
/** Close a file with special handling for stdin or stdout.
 *
 * This will not actually close the file if it is stdin or stdout.
 *
 * \\param file - the stdio file to close. */
LIBRSYNC_EXPORT int rs_file_close(FILE *file);
"""
_librsync.rs_file_close.restype = ctypes.c_int
_librsync.rs_file_close.argtypes = (FILE,)


def fclose(handle: FILE) -> None:
    return _librsync.rs_file_close(handle)


"""
/** Generate the signature of a basis file, and write it out to another.
 *
 * \\param old_file Stdio readable file whose signature will be generated.
 *
 * \\param sig_file Writable stdio file to which the signature will be written./
 *
 * \\param block_len block size for signature generation, in bytes
 *
 * \\param strong_len truncated length of strong checksums, in bytes
 *
 * \\param sig_magic A signature magic number indicating what format to use.
 *
 * \\param stats Optional pointer to receive statistics.
 *
 * \\sa \\ref api_whole */
LIBRSYNC_EXPORT rs_result rs_sig_file(FILE *old_file, FILE *sig_file,
                                      size_t block_len, size_t strong_len,
                                      rs_magic_number sig_magic,
                                      rs_stats_t *stats);
"""
_librsync.rs_sig_file.restype = ctypes.c_int
_librsync.rs_sig_file.argtypes = (
    FILE, FILE, ctypes.c_size_t, ctypes.c_size_t, ctypes.c_int, ctypes.c_void_p
)

"""
/** Load signatures from a signature file into memory.
 *
 * \\param sig_file Readable stdio file from which the signature will be read.
 *
 * \\param sumset on return points to the newly allocated structure.
 *
 * \\param stats Optional pointer to receive statistics.
 *
 * \\sa \\ref api_whole */
LIBRSYNC_EXPORT rs_result rs_loadsig_file(FILE *sig_file,
                                          rs_signature_t **sumset,
                                          rs_stats_t *stats);
"""
_librsync.rs_loadsig_file.restype = ctypes.c_int
_librsync.rs_loadsig_file.argtypes = (
    FILE, ctypes.POINTER(ctypes.c_void_p), ctypes.c_void_p
)

"""
/** Generate a delta between a signature and a new file into a delta file.
 *
 * \\sa \\ref api_whole */
LIBRSYNC_EXPORT rs_result rs_delta_file(rs_signature_t *, FILE *new_file,
                                        FILE *delta_file, rs_stats_t *);
"""
_librsync.rs_delta_file.restype = ctypes.c_int
_librsync.rs_delta_file.argtypes = (ctypes.c_void_p, FILE, FILE, ctypes.c_void_p)

"""
/** Apply a patch, relative to a basis, into a new file.
 *
 * \\sa \\ref api_whole */
LIBRSYNC_EXPORT rs_result rs_patch_file(FILE *basis_file, FILE *delta_file,
                                        FILE *new_file, rs_stats_t *);
"""
_librsync.rs_patch_file.restype = ctypes.c_int
_librsync.rs_patch_file.argtypes = (FILE, FILE, FILE, ctypes.c_void_p)


class LibrsyncError(Exception):
    def __init__(self, result):
        super(LibrsyncError, self).__init__(_librsync.rs_strerror(ctypes.c_int(result)))


def seekable(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        s = args[0]
        assert callable(getattr(s, "seek", None)), (
            "Must provide seekable " "file-like object"
        )
        return f(*args, **kwargs)

    return wrapper


def _execute(job, f, o=None):
    """
    Executes a librsync "job" by reading bytes from `f` and writing results to
    `o` if provided. If `o` is omitted, the output is ignored.
    """
    # Re-use the same buffer for output, we will read from it after each
    # iteration.
    out = ctypes.create_string_buffer(RS_JOB_BLOCKSIZE)
    while True:
        block = f.read(RS_JOB_BLOCKSIZE)
        buff = Buffer()
        # provide the data block via input buffer.
        buff.next_in = char_ptr_from_bytes(block)
        buff.avail_in = ctypes.c_size_t(len(block))
        buff.eof_in = ctypes.c_int(not block)
        # Set up our buffer for output.
        buff.next_out = ctypes.cast(out, CharPtr)
        buff.avail_out = ctypes.c_size_t(RS_JOB_BLOCKSIZE)
        result = _librsync.rs_job_iter(job, ctypes.byref(buff))
        if o:
            o.write(out.raw[: RS_JOB_BLOCKSIZE - buff.avail_out])
        if result == RS_DONE:
            break
        elif result != RS_BLOCKED:
            raise LibrsyncError(result)
        if buff.avail_in > 0:
            # There is data left in the input buffer, librsync did not consume
            # all of it. Rewind the file a bit so we include that data in our
            # next read. It would be better to simply tack data to the end of
            # this buffer, but that is very difficult in Python.
            f.seek(f.tell() - buff.avail_in)
    if o and callable(getattr(o, "seek", None)):
        # As a matter of convenience, rewind the output file.
        o.seek(0)
    return o


def debug(level=syslog.LOG_DEBUG):
    assert level in TRACE_LEVELS, "Invalid log level %i" % level
    _librsync.rs_trace_set_level(level)


@seekable
def signature(f, s=None, block_size=RS_DEFAULT_BLOCK_LEN):
    """
    Generate a signature for the file `f`. The signature will be written to `s`.
    If `s` is omitted, a temporary file will be used. This function returns the
    signature file `s`. You can specify the size of the blocks using the
    optional `block_size` parameter.
    """
    if s is None:
        s = tempfile.SpooledTemporaryFile(max_size=MAX_SPOOL, mode="wb")
    job = _librsync.rs_sig_begin(block_size, RS_DEFAULT_STRONG_LEN)
    try:
        _execute(job, f, s)
    finally:
        _librsync.rs_job_free(job)
    return s


@seekable
def delta(f, s, d=None):
    """
    Create a delta for the file `f` using the signature read from `s`. The delta
    will be written to `d`. If `d` is omitted, a temporary file will be used.
    This function returns the delta file `d`. All parameters must be file-like
    objects.
    """
    if d is None:
        d = tempfile.SpooledTemporaryFile(max_size=MAX_SPOOL, mode="wb")
    sig = ctypes.c_void_p()
    job = _librsync.rs_loadsig_begin(ctypes.byref(sig))
    try:
        _execute(job, s)
    finally:
        _librsync.rs_job_free(job)
    try:
        _librsync.rs_build_hash_table(sig)
        job = _librsync.rs_delta_begin(sig)
        try:
            _execute(job, f, d)
        finally:
            _librsync.rs_job_free(job)
    finally:
        _librsync.rs_free_sumset(sig)
    return d


@seekable
def patch(f, d, o=None):
    """
    Patch the file `f` using the delta `d`. The patched file will be written to
    `o`. If `o` is omitted, a temporary file will be used. This function returns
    the be patched file `o`. All parameters should be file-like objects. `f` is
    required to be seekable.
    """
    if o is None:
        o = tempfile.SpooledTemporaryFile(max_size=MAX_SPOOL, mode="wb")

    @patch_callback
    def read_cb(_, pos, length, buff):
        f.seek(pos)
        block = f.read(length.contents.value)
        if block:
            ctypes.memmove(buff.contents, block, len(block))
            length.contents.value = len(block)
        else:
            length.contents.value = 0
        return RS_DONE

    job = _librsync.rs_patch_begin(read_cb, None)
    try:
        _execute(job, d, o)
    finally:
        _librsync.rs_job_free(job)
    return o


def signature_from_paths(
    base_path: str, sig_path: str,
    block_len: int = RS_DEFAULT_BLOCK_LEN,
    strong_len: int = RS_DEFAULT_STRONG_LEN
) -> bool:
    with resource_manager() as rm:
        base_fp = rm.add(fopen(base_path, "rb"), fclose)
        sig_fp = rm.add(fopen(sig_path, "wb"), fclose)
        return rm.ok() and _librsync.rs_sig_file(
            base_fp, sig_fp, block_len, strong_len, RS_MD4_SIG_MAGIC, None
        ) == 0


@contextmanager
def loadsignature_from_paths(sig_path: str) -> ctypes.c_void_p:
    with resource_manager() as rm:
        sig_fp = rm.add(fopen(sig_path, "rb"), fclose)
        if rm.ok():
            sig = ctypes.c_void_p()
            if _librsync.rs_loadsig_file(sig_fp, ctypes.byref(sig), None) == 0:
                try:
                    _librsync.rs_build_hash_table(sig)
                    yield sig
                finally:
                    _librsync.rs_free_sumset(sig)
            else:
                yield None
        else:
            yield None


def delta_from_paths(sig_path: str, new_path: str, delta_path: str) -> bool:
    with resource_manager() as rm:
        new_fp = rm.add(fopen(new_path, "rb"), fclose)
        delta_fp = rm.add(fopen(delta_path, "wb"), fclose)
        if rm.ok():
            with loadsignature_from_paths(sig_path) as sig:
                return sig and _librsync.rs_delta_file(sig, new_fp, delta_fp, None) == 0

        return False


def patch_from_paths(base_path: str, delta_path: str, result_path: str) -> bool:
    with resource_manager() as rm:
        base_fp = rm.add(fopen(base_path, "rb"), fclose)
        delta_fp = rm.add(fopen(delta_path, "rb"), fclose)
        result_fp = rm.add(fopen(result_path, "wb"), fclose)
        return rm.ok() and _librsync.rs_patch_file(
            base_fp, delta_fp, result_fp, None
        ) == 0

    return False
