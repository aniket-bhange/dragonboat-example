package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/goutils/syncutil"
	// "github.com/lni/dragonboat/v3/config"
	// "github.com/lni/dragonboat/v3/logger"
)

const (
	shardId uint64 = 128
)

var (
	addresses = []string{
		"lcoalhost:6000",
		"lcoalhost:6001",
		"lcoalhost:6002",
	}
	errorNotMemberChanged = errors.New("Not Membership Changed")
)

func splitMembershipChangeCmd(v string) (string, string, uint64, error) {
	parts := strings.Split(v, " ")
	if len(parts) == 2 || len(parts) == 3 {
		cmd := strings.ToLower(strings.TrimSpace(parts[0]))
		if cmd != "add" && cmd != "remove" {
			return "", "", 0, errorNotMemberChanged
		}

		addr := ""
		var nodeIDStr string
		var nodeID uint64
		var err error

		if cmd == "add" {
			addr = strings.TrimSpace(parts[1])
			nodeIDStr = strings.TrimSpace(parts[2])
		} else {
			nodeIDStr = strings.TrimSpace(parts[1])
		}

		if nodeID, err = strconv.ParseUint(nodeIDStr, 10, 64); err != nil {
			return "", "", 0, errorNotMemberChanged
		}

		return cmd, addr, nodeID, nil
	}
	return "", "", 0, errorNotMemberChanged
}

func makeMembershipChange(nh *dragonboat.NodeHost, cmd string, addr string, nodeId uint64) {
	var rs *dragonboat.RequestState
	var err error

	if cmd == "add" {
		rs, err = nh.RequestAddNode(shardId, nodeId, addr, 0, 3*time.Second)
	} else if cmd == "remove" {
		rs, err = nh.RequestDeleteNode(shardId, nodeId, 0, 3*time.Second)
	} else {
		panic("unknown cmd")
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "membership changing failed, %v\n", err)
		return
	}
	select {
	case r := <-rs.CompletedC:
		if r.Completed() {
			fmt.Fprintf(os.Stdout, "membership changed completely \n")
		} else {
			fmt.Fprintf(os.Stderr, "membership change failed\n")
		}
	}

}

func main() {
	replicaId := flag.Int("replicaId", 1, "Replica Id to use")
	addr := flag.String("addr", "", "Node Host Address")
	join := flag.Bool("join", false, "Joining a new node")

	flag.Parse()

	if len(*addr) == 0 && *replicaId != 1 && *replicaId != 2 && *replicaId != 3 {
		fmt.Fprint(os.Stderr, "node must be 1,2,3 when address is not specified\n")
		os.Exit(1)
	}

	if runtime.GOOS == "darwin" {
		signal.Ignore(syscall.Signal(0xd))
	}
	var initialMembers map[uint64]string

	if !*join {
		for idx, v := range addresses {
			initialMembers[uint64(idx+1)] = v
		}
	}

	var nodeAddress string

	if len(*addr) != 0 {
		nodeAddress = *addr
	} else {
		nodeAddress = initialMembers[uint64(*replicaId)]
	}

	fmt.Fprintf(os.Stdout, "node address: %s\n", nodeAddress)

	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)

	rc := config.Config{
		NodeID:      uint64(*replicaId),
		ClusterID:   shardId,
		ElectionRTT: 10,
		CheckQuorum: true,

		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}

	raftLogDir := filepath.Join("example-data", "sample-app", fmt.Sprintf("node%d", *replicaId))

	nhc := config.NodeHostConfig{
		WALDir:         raftLogDir,
		NodeHostDir:    raftLogDir,
		RTTMillisecond: 200,
		RaftAddress:    nodeAddress,
	}

	nh, err := dragonboat.NewNodeHost(nhc)
	// checkError(err)
	if err != nil {
		panic(err)
	}

	if err := nh.StartCluster(initialMembers, *join, NewStateMachine, rc); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add a cluster, %v\n", err)
		os.Exit(1)
	}
	raftStopper := syncutil.NewStopper()
	consoleStopper := syncutil.NewStopper()

	ch := make(chan string, 16)

	consoleStopper.RunWorker(func() {
		reader := bufio.NewReader(os.Stdin)

		for {
			s, err := reader.ReadString('\n')
			if err != nil {
				close(ch)
				return
			}

			if s == "exit\n" {
				raftStopper.Stop()
				nh.Stop()
				return
			}

			ch <- s
		}
	})

	raftStopper.RunWorker(func() {
		cs := nh.GetNoOPSession(shardId)

		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return
				}
				msg := strings.Replace(v, "\n", "", 1)
				if cmd, addr, nodeId, err := splitMembershipChangeCmd(msg); err == nil {
					makeMembershipChange(nh, cmd, addr, nodeId)
				} else {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

					_, err := nh.SyncPropose(ctx, cs, []byte(msg))
					cancel()

					if err != nil {
						fmt.Fprintf(os.Stderr, "Error while creating SyncPropose %v\n", err)
					}
				}
			case <-raftStopper.ShouldStop():
				return

			}
		}
	})
	raftStopper.Wait()
}
