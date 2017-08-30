import os
import shutil
import logging
# logging.basicConfig(level=logging.DEBUG,
# format="%(asctime)s %(threadName)s %(name)s %(message)s")

import paramiko

from atomic_hpc.mockssh import mockserver
import pytest


def files_equal(fname1, fname2):
    if os.stat(fname1).st_size == os.stat(fname2).st_size:
        with open(fname1, "rb") as f1, open(fname2, "rb") as f2:
            if f1.read() == f2.read():
                return True


@pytest.fixture("function")
def server():

    test_folder = os.path.join(os.path.dirname(__file__), 'test_tmp')
    if os.path.exists(test_folder):
        shutil.rmtree(test_folder)
    os.mkdir(test_folder)

    users = {
        "user_key_path": {"private_key_path": os.path.join(os.path.dirname(__file__), 'sample-user-key')},
        "user_password": {"password": "password"}
    }
    with mockserver.Server(users, test_folder) as s:
            yield s


def test_invalid_user(server):
    with pytest.raises(KeyError) as exc:
        server.client("unknown-user")
    assert exc.value.args[0] == "unknown-user"


def test_connect_wrong_hostname(server):
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    with pytest.raises(Exception):
        c.connect(hostname="wrong_hostname",
                  port=server.port,
                  username="user_password",
                  password="password",
                  allow_agent=False,
                  look_for_keys=False,
                  timeout=5)  # NB this hangs for over a minute if no timeout set


def test_connect_wrong_password(server):
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    with pytest.raises(paramiko.ssh_exception.AuthenticationException):
        c.connect(hostname=server.host,
                  port=server.port,
                  username="user_password",
                  password="wrong_password",
                  allow_agent=False,
                  look_for_keys=False)


def test_connect_wrong_keypath(server):
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # should really raise paramiko.ssh_exception.AuthenticationException but doesn't
    # looks related to paramiko/paramiko#387
    with pytest.raises(Exception):
        c.connect(hostname=server.host,
                  port=server.port,
                  username="user_key_path",
                  key_filename=os.path.join(os.path.dirname(__file__), 'wrong-user-key'),
                  allow_agent=False,
                  look_for_keys=False)

def test_connect_password(server):
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    c.connect(hostname=server.host,
              port=server.port,
              username="user_password",
              password="password",
              allow_agent=False,
              look_for_keys=False)
    c.close()

def test_connect_keypath(server):
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    c.connect(hostname=server.host,
              port=server.port,
              username="user_key_path",
              key_filename=os.path.join(os.path.dirname(__file__), 'sample-user-key'),
              allow_agent=False,
              look_for_keys=False)
    c.close()


@pytest.mark.parametrize("uid", ["user_key_path", "user_password"])
def test_ssh_session(server, uid):
    with server.client(uid) as c:
        _, stdout, _ = c.exec_command("ls")
        assert stdout.read().decode() == ""

        _, stdout, _ = c.exec_command("echo hallo")
        assert stdout.read().decode().strip() == "hallo"


@pytest.mark.parametrize("uid", ["user_key_path", "user_password"])
def test_sftp_session(server, uid):
    with server.client(uid) as c:
        sftp = c.open_sftp()
        sftp.mkdir("folder")
        sftp.chdir("folder")
        fpath = os.path.join(os.path.dirname(__file__), "test_tmp", "folder", "test.txt")
        assert not os.access(fpath, os.F_OK)
        sftp.put(__file__, "test.txt", confirm=True)
        assert files_equal(__file__, fpath)
        sftp.mkdir("other")
        with pytest.raises(IOError):
            sftp.mkdir("other")
        assert sorted(sftp.listdir()) == ['other', 'test.txt']

        # the actual ssh client does not change its directory inline with the sftp client
        # so exec commands will always run from the initial root directory
        _, stdout, _ = c.exec_command("ls")
        assert stdout.read().decode().strip().split() == ["folder"]
        # to change folder we must use an initial change dir command
        _, stdout, _ = c.exec_command("cd folder; ls")
        assert sorted(stdout.read().decode().strip().split()) == ['other', 'test.txt']

        sftp.unlink("test.txt")
        assert sftp.listdir() == ['other']
        sftp.rmdir("other")
        assert sftp.listdir() == []
        with pytest.raises(IOError):
            sftp.rmdir("other")
        sftp.chdir("..")
        assert sftp.listdir() == ['folder']
