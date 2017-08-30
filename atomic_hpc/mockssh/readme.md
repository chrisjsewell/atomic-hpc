# mockssh

this module mocks a remote host with a local folder path

It is based on https://github.com/carletes/mock-ssh-server/tree/master/mockssh
with additions made (to implement more sftp functions) based on https://github.com/rspivak/sftpserver

The following changes have also been made:

- revised `users` parameter, such that either a private_path_key or password can be used
- added a `dirname` parameter to the `Server` context manager, such that the this will be set as the root path
  for the duration of the context.
- patched `paramiko.sftp_client.SFTPClient.chdir` to fix its use with relative paths 
  (I have checked that this works fine when connecting to a real remote host)

See test_mockssh.py for example use. 

Some links that may be helpful:

- https://www.programcreek.com/python/example/4561/paramiko.SSHClient
- http://jessenoller.com/blog/2009/02/05/ssh-programming-with-paramiko-completely-different
- https://stackoverflow.com/questions/15579117/paramiko-using-encrypted-private-key-file-on-os-x
- https://wiki.ch.ic.ac.uk/wiki/index.php?title=Mod:Hunt_Research_Group/SSHkeyfile
- https://stackoverflow.com/questions/850749/check-whether-a-path-exists-on-a-remote-host-using-paramiko
- https://stackoverflow.com/questions/25260088/paramiko-with-continuous-stdout
- https://stackoverflow.com/questions/760978/long-running-ssh-commands-in-python-paramiko-module-and-how-to-end-them
- https://stackoverflow.com/questions/36490989/how-to-keep-ssh-session-not-expired-using-paramiko