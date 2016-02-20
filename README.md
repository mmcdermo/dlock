# dlock
Dlock is a distributed lock library written in Go. You can use it to coordinate access to resources between servers by ensuring only one server holds a particular lock at a given time.

# Usage
To run a dlock server, you can use the following code:

    go dlock_server.RunServer("localhost", "8422")
    m.Run()

Then to connect as a client and block until a lock is free, you can simply:

    conn, err := dlock_client.Connect("localhost", "8422")
    lock_name := "lock_test"
    client_name := "Trillby"
    b := dlock_client.AcquireLock(conn, "lock_test", client_name)
