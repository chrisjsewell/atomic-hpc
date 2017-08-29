# mockssh

this module mocks a remote host with a local folder path

It is based on https://github.com/carletes/mock-ssh-server/tree/master/mockssh
with additions made (to implement more sftp functions) based on https://github.com/rspivak/sftpserver

The following changes have also been made:

- revised `users` parameter, such that either a private_path_key or password can be used
- added a `dirname` parameter to the `Server` context manager, such that the this will be set as the root path
  for the duration of the context.
- patched `paramiko.sftp_client.SFTPClient.chdir` to fix its use with relative paths.

See test_mockssh.py for example use. 