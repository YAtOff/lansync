## Regarding SO_REUSEADDR

Enable port reusage so we will be able to run multiple clients and servers on single (host, port).
Do not use socket.SO_REUSEADDR except you using linux(kernel<3.9): goto https://stackoverflow.com/questions/14388706/how-do-so-reuseaddr-and-so-reuseport-differ for more information.
For linux hosts all sockets that want to share the same address and port combination must belong to processes that share the same effective user ID!
So, on linux(kernel>=3.9) you have to run multiple servers and clients under one user to share the same (host, port).
Thanks to @stevenreddie
