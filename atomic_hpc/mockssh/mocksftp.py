"""
Copyright (c) 2016 Carlos Valiente

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import logging
import os

from errno import EACCES, EDQUOT, EPERM, EROFS, ENOENT, ENOTDIR

import paramiko

__all__ = [
    "SFTPServer",
]


class SFTPHandle(paramiko.SFTPHandle):

    log = logging.getLogger(__name__)

    def __init__(self, file_obj, flags=0):
        super(SFTPHandle, self).__init__(flags)
        self.file_obj = file_obj

    @property
    def readfile(self):
        return self.file_obj

    @property
    def writefile(self):
        return self.file_obj


LOG = logging.getLogger(__name__)


def returns_sftp_error(func):

    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except OSError as err:
            LOG.debug("Error calling %s(%s, %s): %s",
                      func, args, kwargs, err, exc_info=True)
            errno = err.errno
            if errno in {EACCES, EDQUOT, EPERM, EROFS}:
                return paramiko.SFTP_PERMISSION_DENIED
            if errno in {ENOENT, ENOTDIR}:
                return paramiko.SFTP_NO_SUCH_FILE
            return paramiko.SFTP_FAILURE
        except Exception as err:
            LOG.debug("Error calling %s(%s, %s): %s",
                      func, args, kwargs, err, exc_info=True)
            return paramiko.SFTP_FAILURE

    return wrapped


class SFTPServerInterface(paramiko.SFTPServerInterface):

    log = logging.getLogger(__name__)

    # ROOT = os.getcwd()
    # def _realpath(self, path):
    #     return self.ROOT + self.canonicalize(path)

    def __init__(self, server, *largs, **kwargs):
        super(SFTPServerInterface, self).__init__(server, *largs, **kwargs)

    def session_started(self):
        pass

    def session_ended(self):
        pass

    @returns_sftp_error
    def open(self, path, flags, attr):
        fd = os.open(path, flags)
        self.log.debug("open(%s): fd: %d", path, fd)
        if flags & (os.O_WRONLY | os.O_RDWR):
            mode = "w"
        elif flags & (os.O_APPEND):
            mode = "a"
        else:
            mode = "r"
        mode += "b"
        self.log.debug("open(%s): Mode: %s", path, mode)
        return SFTPHandle(os.fdopen(fd, mode), flags)

    @returns_sftp_error
    def stat(self, path):
        st = os.stat(path)
        return paramiko.SFTPAttributes.from_stat(st, path)

    @returns_sftp_error
    def chattr(self, path, attr):
        if hasattr(attr, "st_mode"):
            os.chmod(path, attr.st_mode)
        # for param in ["st_size", "st_uid", "st_gid", "st_atime", "st_mtime"]:
        #     if hasattr(attr, param):
        #         return paramiko.SFTP_OP_UNSUPPORTED
        return paramiko.SFTP_OK

    @returns_sftp_error
    def list_folder(self, path):
        """Looks up folder contents of `path.`"""
        folder_contents = []
        for f in os.listdir(path):
            attr = paramiko.SFTPAttributes.from_stat(os.stat(os.path.join(path, f)))
            attr.filename = f
            folder_contents.append(attr)
        return folder_contents

    @returns_sftp_error
    def mkdir(self, path, attr):
        os.mkdir(path)
        return paramiko.SFTP_OK

    @returns_sftp_error
    def remove(self, path):
        os.remove(path)
        return paramiko.SFTP_OK

    @returns_sftp_error
    def rename(self, oldpath, newpath):
        os.rename(oldpath, newpath)
        return paramiko.SFTP_OK

    @returns_sftp_error
    def rmdir(self, path):
        os.rmdir(path)
        return paramiko.SFTP_OK


class SFTPServer(paramiko.SFTPServer):

    def __init__(self, channel, name, server,
                 *args, **kwargs):
        kwargs["sftp_si"] = SFTPServerInterface
        super(SFTPServer, self).__init__(channel, name, server, *args,
                                         **kwargs)

