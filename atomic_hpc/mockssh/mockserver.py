import logging
import os
import select
import socket
import subprocess
import threading

try:
    from queue import Queue
except ImportError:  # Python 2.7
    from Queue import Queue

import paramiko

from atomic_hpc.mockssh.mocksftp import SFTPServer


__all__ = [
    "Server",
]


SERVER_KEY_PATH = os.path.join(os.path.dirname(__file__), "server-key")


class Handler(paramiko.ServerInterface):

    log = logging.getLogger(__name__)

    def __init__(self, server, client_conn):
        self.server = server
        self.thread = None
        self.command_queues = {}
        client, _ = client_conn
        self.transport = t = paramiko.Transport(client)
        t.add_server_key(paramiko.RSAKey(filename=SERVER_KEY_PATH))
        t.set_subsystem_handler("sftp", SFTPServer)

    def run(self):
        self.transport.start_server(server=self)
        while True:
            channel = self.transport.accept()
            if channel is None:
                break
            if channel.chanid not in self.command_queues:
                self.command_queues[channel.chanid] = Queue()
            t = threading.Thread(target=self.handle_client, args=(channel,))
            t.setDaemon(True)
            t.start()

    def handle_client(self, channel):
        try:
            command = self.command_queues[channel.chanid].get(block=True)
            self.log.debug("Executing %s", command)
            p = subprocess.Popen(command, shell=True,
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            channel.sendall(stdout)
            channel.sendall_stderr(stderr)
            channel.send_exit_status(p.returncode)
        except Exception:
            self.log.error("Error handling client (channel: %s)", channel,
                           exc_info=True)
        finally:
            channel.close()

    def check_auth_password(self, username, password):
        if username not in self.server._users:
            self.log.debug("Unknown user '%s'", username)
            return paramiko.AUTH_FAILED
        if "password" not in self.server._users[username]:
            self.log.debug("No password set for user '%s'", username)
            return paramiko.AUTH_FAILED
        known_password = self.server._users[username]["password"]
        if known_password == password:
            self.log.debug("Accepting password for user '%s'", username)
            return paramiko.AUTH_SUCCESSFUL
        self.log.debug("Rejecting password for user '%s'", username)
        return paramiko.AUTH_FAILED

    def check_auth_publickey(self, username, key):
        if username not in self.server._users:
            self.log.debug("Unknown user '%s'", username)
            return paramiko.AUTH_FAILED
        if "private_key" not in self.server._users[username]:
            self.log.debug("No public key set for user '%s'", username)
            return paramiko.AUTH_FAILED
        known_public_key = self.server._users[username]["private_key"]
        if known_public_key == key:
            self.log.debug("Accepting public key for user '%s'", username)
            return paramiko.AUTH_SUCCESSFUL
        self.log.debug("Rejecting public ley for user '%s'", username)
        return paramiko.AUTH_FAILED

    def check_channel_exec_request(self, channel, command):
        self.command_queues.setdefault(channel.get_id(), Queue()).put(command)
        return True

    def check_channel_request(self, kind, chanid):
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def get_allowed_auths(self, username):
        return "publickey,password"

try:
    from unittest import mock
except:
    import mock
import stat
import errno
from paramiko.py3compat import b
from paramiko.sftp import SFTPError


def patch_chdir(self, path=None):
    """
    STANDARD IMPLEMENTATION DOESN'T WORK WITH RELATIVE PATHS

    Change the "current directory" of this SFTP session.  Since SFTP
    doesn't really have the concept of a current working directory, this is
    emulated by Paramiko.  Once you use this method to set a working
    directory, all operations on this `.SFTPClient` object will be relative
    to that path. You can pass in ``None`` to stop using a current working
    directory.

    :param str path: new current working directory

    :raises IOError: if the requested path doesn't exist on the server

    .. versionadded:: 1.4
    """
    if path is None:
        self._cwd = None
        return
    if not stat.S_ISDIR(self.stat(path).st_mode):
        raise SFTPError(errno.ENOTDIR, "%s: %s" % (os.strerror(errno.ENOTDIR), path))
    # self._cwd = b(self.normalize(path))
    if self._cwd is None or os.path.isabs(path):
        self._cwd = b(self.normalize(os.path.abspath(path)))
    else:
        cwd = self._cwd.decode()
        self._cwd = b(self.normalize(os.path.join(cwd, path)))


class Server(object):

    host = "localhost"

    log = logging.getLogger(__name__)

    def __init__(self, users, dirname):
        self._dirname = dirname
        self._socket = None
        self._thread = None
        self._users = {}
        for uid, login in users.items():
            self.add_user(uid, login)

    def add_user(self, uid, login):
        """

        Parameters
        ----------
        uid: str
            the username
        login: dict
            login details with one of keys: ["private_key_path", "password"]

        Returns
        -------

        """
        if "private_key_path" in login:
            login["private_key"] = paramiko.RSAKey.from_private_key_file(login["private_key_path"])
        self._users[uid] = login

    def __enter__(self):
        self._cwd = os.getcwd()
        os.chdir(self._dirname)
        self._socket = s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.host, 0))
        s.listen(5)
        self._thread = t = threading.Thread(target=self._run)
        t.setDaemon(True)
        t.start()
        return self

    @mock.patch("paramiko.sftp_client.SFTPClient.chdir", new=patch_chdir)
    def _run(self):
        sock = self._socket
        while sock.fileno() > 0:
            self.log.debug("Waiting for incoming connections ...")
            rlist, _, _ = select.select([sock], [], [], 1.0)
            if rlist:
                conn, addr = sock.accept()
                self.log.debug("... got connection %s from %s", conn, addr)
                handler = Handler(self, (conn, addr))
                t = threading.Thread(target=handler.run)
                t.setDaemon(True)
                t.start()

    def __exit__(self, *exc_info):
        os.chdir(self._cwd)
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
        except Exception:
            pass
        self._socket = None
        self._thread = None

    def client(self, uid):
        login = self._users[uid]
        c = paramiko.SSHClient()
        host_keys = c.get_host_keys()
        key = paramiko.RSAKey.from_private_key_file(SERVER_KEY_PATH)
        host_keys.add(self.host, "ssh-rsa", key)
        host_keys.add("[%s]:%d" % (self.host, self.port), "ssh-rsa", key)
        c.set_missing_host_key_policy(paramiko.RejectPolicy())
        #c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        c.connect(hostname=self.host,
                  port=self.port,
                  username=uid,
                  password=login.get("password", None),
                  key_filename=login.get("private_key_path", None),
                  allow_agent=False,
                  look_for_keys=False)
        return c

    @property
    def port(self):
        return self._socket.getsockname()[1]

    @property
    def users(self):
        return self._users.keys()
