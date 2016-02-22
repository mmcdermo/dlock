package dlock

import (
	"strings"
	"time"
)

type Cluster struct  {
	cluster_connections []*Connection
}

func ClusterInitialize(hosts []string) (Cluster, error) {
	cluster_connections := make([]*Connection, 0)
	for _, host := range hosts {
		hostParts := strings.Split(host, ":")
		conn, err := Connect(hostParts[0], hostParts[1])
		if err != nil {
			return Cluster{cluster_connections}, err
		}
		cluster_connections = append(cluster_connections, conn)
	}
	return Cluster{cluster_connections}, nil
}

//Disconnect from all servers in cluster
func (c *Cluster) ClusterShutdown() {
	for _, conn := range c.cluster_connections {
		conn.Close()
	}
}

// Block until we've acquired a majority of locks
func (c *Cluster) AcquireLock(lock_name string, entity string) []string {
	success := false
	statuses := make([]string, 0)
	for false == success {
		if len(statuses) != 0 {
			time.Sleep(250 * time.Millisecond)
		}
		success, statuses = c.TryAcquireLock(lock_name, entity)
	}
	return statuses
}

// Attempt to acquire lock
func (c *Cluster) TryAcquireLock(lock_name string, entity string) (bool, []string) {
	owners := make([]string, 0)
	success_count := 0
	error := false
	for _, conn := range c.cluster_connections {
		status := ClientTryAcquireLock(conn, lock_name, entity)
		if "lock_acquired" != status {
			statusParts := strings.Split(status, "=")
			if statusParts[0] == "owner" {
				owners = append(owners, statusParts[1])
			} else {
				error = true
				owners = append(owners, "Error: "+statusParts[0])
			}
		} else {
			success_count += 1
		}
	}

	n := len(c.cluster_connections)
	return !error && success_count >= (n+1)/2, owners
}

func (c *Cluster) ReleaseLock(lock_name string, entity string) (bool, []string) {
	statuses := make([]string, 0)
	released := true
	for _, conn := range c.cluster_connections {
		status := ClientReleaseLock(conn, lock_name, entity)
		if "lock_released" != status {
			released = false
		}
		statuses = append(statuses, status)
	}
	return released, statuses
}
