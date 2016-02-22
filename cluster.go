package dlock

import (
	"strings"
	_ "fmt"
)

type cluster struct  {
	cluster_connections []*Connection
}

func ClusterInitialize(hosts []string) (cluster, error) {
	cluster_connections := make([]*Connection, 0)
	for _, host := range hosts {
		hostParts := strings.Split(host, ":")
		conn, err := Connect(hostParts[0], hostParts[1])
		if err != nil {
			return cluster{cluster_connections}, err
		}
		cluster_connections = append(cluster_connections, conn)
	}
	return cluster{cluster_connections}, nil
}

func (c *cluster) AcquireLock(lock_name string, entity string) (bool, []string) {
	statuses := make([]string, 0)
	success_count := 0
	for _, conn := range c.cluster_connections {
		status := ClientAcquireLock(conn, lock_name, entity)
		if "lock_acquired" == status {
			success_count += 1
		}
		statuses = append(statuses, status)
	}
	n := len(c.cluster_connections)
	return success_count >= (n+1)/2, statuses
}

func (c *cluster) TryAcquireLock(lock_name string, entity string) (bool, []string) {
	owners := make([]string, 0)
	success_count := 0
	for _, conn := range c.cluster_connections {
		status := ClientTryAcquireLock(conn, lock_name, entity)
		if "lock_acquired" != status {
			statusParts := strings.Split(status, "=")
			owners = append(owners, statusParts[1])
		}
	}
	n := len(c.cluster_connections)
	return success_count >= (n+1)/2, owners
}

func (c *cluster) ReleaseLock(lock_name string, entity string) (bool, []string) {
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
