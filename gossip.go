package main

import (
	"flag"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func randomChoice(items []string, n int) []string {
	if len(items) == 0 {
		return []string{}
	}
	if len(items) < n {
		n = len(items)
	}
	seen := make(map[uint32]bool)
	ret := make([]string, 0, n)
	for len(seen) < n {
		idx := rand.Uint32() % uint32(len(items))
		if _, ok := seen[idx]; !ok {
			seen[idx] = true
			ret = append(ret, items[idx])
		}
	}
	return ret
}

func main() {
	bootstrapNode := flag.String("bootstrap", "", "host:port for a bootstrap node")
	listenAddr := flag.String("addr", ":10001", "listen for updates on this address")
	fanout := flag.Uint("fanout", 2, "fanout of the gossip protocol")
	periodSeconds := flag.Duration("period", 3, "gossip period in seconds")

	flag.Parse()

	if *bootstrapNode != "" {
		log.Printf("bootstrapping with %s", *bootstrapNode)
	} else {
		log.Printf("-bootstrap not given, starting standalone")
	}

	addr, err := net.ResolveUDPAddr("udp", *listenAddr)
	if err != nil {
		log.Fatal("Error resolving UDP address for self: " + err.Error())
	}

	log.Printf("Resolved address to %v", addr)

	srvConn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal("Error listening: " + err.Error())
	}
	defer srvConn.Close()

	var membersMutex sync.RWMutex
	members := map[string]bool{
		*listenAddr: true,
	}

	if *bootstrapNode != "" {
		members[*bootstrapNode] = true
	}

	stop := make(chan struct{})
	go func(stop chan struct{}) {
		buf := make([]byte, 1024)
		for {
			select {
			case <-stop:
				break
			default:
				n, fromAddr, err := srvConn.ReadFromUDP(buf)
				if n > 0 {
					membersMutex.Lock()
					addressesStr := strings.Trim(string(buf[:n]), " \n\t")
					addresses := strings.Split(addressesStr, ",")
					log.Printf("Received %d addresses from %v: %v", len(addresses), fromAddr, addresses)
					for _, address := range addresses {
						members[address] = true
					}
					membersMutex.Unlock()
				}
				if err != nil {
					log.Fatalf("Receive error on UDP connection: %v", err.Error())
				}
			}
		}
	}(stop)

	defer func() {
		stop <- struct{}{}
	}()

	// Every periodSeconds seconds, contact `fanout` random members from our list
	// with our membership list.
	ticker := time.NewTicker(time.Second * *periodSeconds)
	defer ticker.Stop()

	for _ = range ticker.C {

		membersMutex.RLock()
		nMembers := len(members)
		// Holds addresses of members to contact
		peerAddresses := make([]string, 0, nMembers-1)
		addrsToSend := make([]string, 0, nMembers)
		for a, _ := range members {
			if a != *listenAddr {
				peerAddresses = append(peerAddresses, a)
			}
			addrsToSend = append(addrsToSend, a)
		}
		membersMutex.RUnlock()

		log.Printf("tick: we know about %d (including us): %v", len(addrsToSend), addrsToSend)
		log.Printf("going to poke %d of %d peers", minInt(int(len(peerAddresses)), int(*fanout)), len(peerAddresses))

		chosen := randomChoice(peerAddresses, int(*fanout))
		log.Printf("found these: %v", chosen)

		// stick our address in the message
		msg := []byte(strings.Join(addrsToSend, ","))
		for _, addrStr := range chosen {
			theirAddr, err := net.ResolveUDPAddr("udp", addrStr)
			if err != nil {
				log.Printf("Could NOT resolve %v as a valid UDP address, continuing", addrStr)
				continue
			}
			log.Printf("Going to talk to %v", theirAddr)

			conn, err := net.DialUDP("udp", nil, theirAddr)
			if err != nil {
				log.Printf("Error dialing %v: %v, continuing", theirAddr, err.Error())
				continue
			}

			_, err = conn.Write(msg)
			if err != nil {
				log.Printf("Error sending message to  %s: %s, continuing", addrStr, err.Error())
				continue
			}
			conn.Close()
		}
	}
}
