package sshconn

import (
	"bufio"
	"bytes"
	"context"
	jsonv2 "encoding/json/v2"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
)

// What a script's reader keeps: the longest line of its output it reads
// (Next), and the most of its standard error it keeps (Script.Stderr).
const (
	maxLine = 4 << 20
	maxSaid = 16 << 10
)

// settleTime is how long Settle waits for a script to end.
const settleTime = 5 * time.Second

// A Script is a script that sh runs on a host (Conn.Script).
type Script struct {
	ctx    context.Context
	s      *Session
	stdin  io.WriteCloser
	out    *bufio.Reader
	stderr limitedBuffer
	stop   func() bool

	// written is the error of writing the script, which Wait returns
	// where sh's end doesn't say why.
	written  error
	waitOnce sync.Once
	waitErr  error
}

// Script runs script on the host c reaches, over an exec session whose
// command is /bin/sh -s: all that the user's login shell reads, whatever
// shell it is, csh among them, which passes it to sh as it is. sh reads
// the script from its standard input, which takes any text, so a value
// goes into it quoted for sh (Quote). The script is one of sh's complete
// commands, as commands joined by semicolons are, which sh reads in full
// before it runs any of it.
//
// What the caller writes after it (Write) is for the script's commands
// to read, once the script has said that it is ready for it, as a line
// of its output (Next), and not before: sh, as dash's and BusyBox's, can
// read ahead of the command it runs, and would run what it read as
// script. CloseWrite ends it.
//
// What a login script prints before the script runs is told from what the
// script says by Next. The session is closed once ctx is done, or by
// Close.
func (c *Conn) Script(ctx context.Context, script string) (*Script, error) {
	s, err := c.newSession(ctx)
	if err != nil {
		return nil, err
	}
	sc := &Script{ctx: ctx, s: s, stderr: limitedBuffer{max: maxSaid}}
	stdin, err := s.StdinPipe()
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	stdout, err := s.StdoutPipe()
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	sc.stdin, sc.out, s.Stderr = stdin, bufio.NewReaderSize(stdout, 64<<10), &sc.stderr
	if err := s.Start("/bin/sh -s"); err != nil {
		_ = s.Close()
		return nil, err
	}
	sc.stop = context.AfterFunc(ctx, func() { _ = s.Close() })
	// One that can't be written has ended, which reading what it said,
	// and Wait, find.
	_, sc.written = io.WriteString(stdin, script+"\n")
	return sc, nil
}

// Write writes p to the script's standard input, after the script.
func (sc *Script) Write(p []byte) (int, error) { return sc.stdin.Write(p) }

// CloseWrite ends the script's standard input: sh ends once it has run
// the script.
func (sc *Script) CloseWrite() error { return sc.stdin.Close() }

// Wait waits for the script to end, and returns how it did: nil for one
// that exited 0, an error for one that didn't, or ctx's error once the
// context the script runs in is done.
func (sc *Script) Wait() error {
	sc.waitOnce.Do(func() {
		sc.waitErr = sc.s.Wait()
		if sc.waitErr == nil {
			sc.waitErr = sc.written
		}
	})
	if err := sc.ctx.Err(); err != nil {
		return err
	}
	return sc.waitErr
}

// Settle waits for the script to end, for a while (settleTime): one that
// holds its standard output and error open longer, as one a command it
// started holds, is ended with the session.
func (sc *Script) Settle() {
	done := make(chan struct{})
	go func() {
		_ = sc.Wait()
		close(done)
	}()
	timer := time.NewTimer(settleTime)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		_ = sc.s.Close()
		<-done
	}
}

// Stderr returns what the script wrote to its standard error: its first
// 16 KiB (maxSaid), which is whole once it has ended.
func (sc *Script) Stderr() string { return sc.stderr.String() }

// Close closes the script's session.
func (sc *Script) Close() error {
	sc.stop()
	return sc.s.Close()
}

// Next returns the next of what the script says that ours takes: a line
// of its output that is a JSON object, which decodes as a T. A line that
// isn't, as a login script's, is skipped, as is one ours doesn't take. It
// is io.EOF once the script's output has ended.
func Next[T any](sc *Script, ours func(*T) bool) (*T, error) {
	for {
		line, err := readLine(sc.out)
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(line, []byte("{")) {
			continue
		}
		var v T
		if jsonv2.Unmarshal(line, &v) == nil && ours(&v) {
			return &v, nil
		}
	}
}

// errLongLine is the error of a script that wrote a line longer than Next
// reads.
var errLongLine = errors.New("it wrote a line longer than a message can be")

// readLine reads a line, without its white space, at most maxLine bytes
// of it.
func readLine(r *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		chunk, err := r.ReadSlice('\n')
		line = append(line, chunk...)
		if len(line) > maxLine {
			return nil, errLongLine
		}
		switch {
		case err == nil:
			return bytes.TrimSpace(line), nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF) && len(bytes.TrimSpace(line)) > 0:
			return bytes.TrimSpace(line), nil
		default:
			return nil, err
		}
	}
}

// Quote quotes s for sh: in single quotes, each single quote in it
// ending them, escaped with a backslash, and starting them again.
func Quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// A limitedBuffer keeps the first max bytes written to it, and takes the
// rest without keeping it.
type limitedBuffer struct {
	mu  sync.Mutex
	b   bytes.Buffer
	max int
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if room := l.max - l.b.Len(); room > 0 {
		l.b.Write(p[:min(len(p), room)])
	}
	return len(p), nil
}

func (l *limitedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
