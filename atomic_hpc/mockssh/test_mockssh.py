import os
import shutil
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


@pytest.mark.parametrize("uid", ["user_key_path", "user_password"])
def test_ssh_session(server, uid):
    with server.client(uid) as c:
        _, stdout, _ = c.exec_command("ls")
        assert stdout.read().decode() == ""


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
        assert sftp.listdir() == ['other', 'test.txt']
        sftp.unlink("test.txt")
        assert sftp.listdir() == ['other']
        sftp.rmdir("other")
        assert sftp.listdir() == []
        with pytest.raises(IOError):
            sftp.rmdir("other")
        sftp.chdir("..")
        assert sftp.listdir() == ['folder']
