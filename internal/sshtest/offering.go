package sshtest

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
)

// Algorithms are what a host offers in its key exchange's first message,
// each list in its order of preference.
type Algorithms struct {
	KeyExchanges []string
	HostKeys     []string
	Ciphers      []string
	MACs         []string
}

// Offering starts a host that offers algs, and does nothing more: it
// sends its version and its key exchange's first message, then reads
// until the client closes the connection. So it can offer algorithms the
// test's process couldn't run, as a host does that a client in FIPS
// 140-only mode has none in common with. It returns the host's address,
// as host:port, and stops when the test ends.
func Offering(t testing.TB, algs Algorithms) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() {
				defer c.Close()
				offer(c, algs)
			})
		}
	})
	return ln.Addr().String()
}

// offer says the host's version, reads the client's, and sends a
// KEXINIT that offers algs (RFC 4253, 7.1), unencrypted, as the first
// packet is.
func offer(c net.Conn, algs Algorithms) {
	if _, err := io.WriteString(c, "SSH-2.0-sshtest\r\n"); err != nil {
		return
	}
	r := bufio.NewReader(c)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		if strings.HasPrefix(line, "SSH-") {
			break
		}
	}
	var payload []byte
	payload = append(payload, 20) // SSH_MSG_KEXINIT
	cookie := make([]byte, 16)
	_, _ = rand.Read(cookie)
	payload = append(payload, cookie...)
	for _, list := range [][]string{
		algs.KeyExchanges, algs.HostKeys, algs.Ciphers, algs.Ciphers, algs.MACs, algs.MACs,
		{"none"},
		{"none"},
		nil, nil,
	} {
		s := strings.Join(list, ",")
		payload = binary.BigEndian.AppendUint32(payload, uint32(len(s))) //nolint:gosec // a short list
		payload = append(payload, s...)
	}
	payload = append(payload, 0)                        // first_kex_packet_follows
	payload = binary.BigEndian.AppendUint32(payload, 0) // reserved
	padding := 8 - (4+1+len(payload))%8                 // to a multiple of 8,
	if padding < 4 {                                    // of at least 4
		padding += 8
	}
	packet := binary.BigEndian.AppendUint32(nil, uint32(1+len(payload)+padding)) //nolint:gosec // a short packet
	packet = append(packet, byte(padding))
	packet = append(packet, payload...)
	packet = append(packet, make([]byte, padding)...)
	if _, err := c.Write(packet); err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, r)
}
