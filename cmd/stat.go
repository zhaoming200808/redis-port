// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/CodisLabs/redis-port/pkg/libs/atomic2"
	"github.com/CodisLabs/redis-port/pkg/libs/log"
	"github.com/CodisLabs/redis-port/pkg/rdb"
)

type cmdStat struct {
	rbytes, wbytes, nentry atomic2.Int64
}

type cmdStatStat struct {
	rbytes, wbytes, nentry int64
}

func (cmd *cmdStat) Stat() *cmdStatStat {
	return &cmdStatStat{
		rbytes: cmd.rbytes.Get(),
		wbytes: cmd.wbytes.Get(),
		nentry: cmd.nentry.Get(),
	}
}

func (cmd *cmdStat) Main() {
	input, output := args.input, args.output
	if len(input) == 0 {
		input = "/dev/stdin"
	}
	if len(output) == 0 {
		output = "/dev/stdout"
	}

	log.Infof("decode from '%s' to '%s'\n", input, output)

	var readin io.ReadCloser
	var nsize int64
	if input != "/dev/stdin" {
		readin, nsize = openReadFile(input)
		defer readin.Close()
	} else {
		readin, nsize = os.Stdin, 0
	}

	var saveto io.WriteCloser
	if output != "/dev/stdout" {
		saveto = openWriteFile(output)
		defer saveto.Close()
	} else {
		saveto = os.Stdout
	}

	reader := bufio.NewReaderSize(readin, ReaderBufferSize)
	writer := bufio.NewWriterSize(saveto, WriterBufferSize)

	ipipe := newRDBLoader(reader, &cmd.rbytes, args.parallel*32)
	opipe := make(chan string, cap(ipipe))

	go func() {
		defer close(opipe)
		group := make(chan int, args.parallel)
		for i := 0; i < cap(group); i++ {
			go func() {
				defer func() {
					group <- 0
				}()
				cmd.decoderMain(ipipe, opipe)
			}()
		}
		for i := 0; i < cap(group); i++ {
			<-group
		}
	}()

	wait := make(chan struct{})
	go func() {
		defer close(wait)
		for s := range opipe {
			cmd.wbytes.Add(int64(len(s)))
			if _, err := writer.WriteString(s); err != nil {
				log.PanicError(err, "write string failed")
			}
			flushWriter(writer)
		}
	}()

	for done := false; !done; {
		select {
		case <-wait:
			done = true
		case <-time.After(time.Second):
		}
		stat := cmd.Stat()
		var b bytes.Buffer
		fmt.Fprintf(&b, "decode: ")
		if nsize != 0 {
			fmt.Fprintf(&b, "total = %d - %12d [%3d%%]", nsize, stat.rbytes, 100*stat.rbytes/nsize)
		} else {
			fmt.Fprintf(&b, "total = %12d", stat.rbytes)
		}
		fmt.Fprintf(&b, "  write=%-12d", stat.wbytes)
		fmt.Fprintf(&b, "  entry=%-12d", stat.nentry)
		log.Info(b.String())
	}
	log.Info("decode: done")
}

type KeyStat struct {
	DB         uint32 `json:"db"`
	Type       string `json:"type"`
	ExpireAt   uint64 `json:"expireat"`
	Key        string `json:"key"`
	FieldCount int    `json:"fieldCount"`
	KeySize    int    `json:"keySize"`
	ValueSize  int    `json:"valueSize"`
}

func (cmd *cmdStat) decoderMain(ipipe <-chan *rdb.BinEntry, opipe chan<- string) {
	toText := func(p []byte) string {
		var b bytes.Buffer
		for _, c := range p {
			switch {
			case c >= '#' && c <= '~':
				b.WriteByte(c)
			default:
				b.WriteByte('.')
			}
		}
		return b.String()
	}
	toJson := func(o interface{}) string {
		b, err := json.Marshal(o)
		if err != nil {
			log.PanicError(err, "encode to json failed")
		}
		return string(b)
	}
	for e := range ipipe {
		o, err := rdb.DecodeDump(e.Value)
		if err != nil {
			log.PanicError(err, "decode failed")
		}

		var b bytes.Buffer
		switch obj := o.(type) {
		default:
			log.Panicf("unknown object %v", o)
		case rdb.String:
			o := &KeyStat{
				DB:         e.DB,
				Type:       "string",
				ExpireAt:   e.ExpireAt,
				Key:        toText(e.Key),
				FieldCount: 1,
				KeySize:    len(e.Key),
				ValueSize:  len(e.Value),
			}
			fmt.Fprintf(&b, "%s\n", toJson(o))
		case rdb.Hash:
			o := &KeyStat{
				DB:         e.DB,
				Type:       "hash",
				ExpireAt:   e.ExpireAt,
				Key:        toText(e.Key),
				FieldCount: len(obj),
				KeySize:    len(e.Key),
				ValueSize:  len(e.Value),
			}
			fmt.Fprintf(&b, "%s\n", toJson(o))
		case rdb.List:
			o := &KeyStat{
				DB:         e.DB,
				Type:       "list",
				ExpireAt:   e.ExpireAt,
				Key:        toText(e.Key),
				FieldCount: len(obj),
				KeySize:    len(e.Key),
				ValueSize:  len(e.Value),
			}
			fmt.Fprintf(&b, "%s\n", toJson(o))
		case rdb.Set:
			o := &KeyStat{
				DB:         e.DB,
				Type:       "set",
				ExpireAt:   e.ExpireAt,
				Key:        toText(e.Key),
				FieldCount: len(obj),
				KeySize:    len(e.Key),
				ValueSize:  len(e.Value),
			}
			fmt.Fprintf(&b, "%s\n", toJson(o))
		case rdb.ZSet:
			o := &KeyStat{
				DB:         e.DB,
				Type:       "zset",
				ExpireAt:   e.ExpireAt,
				Key:        toText(e.Key),
				FieldCount: len(obj),
				KeySize:    len(e.Key),
				ValueSize:  len(e.Value),
			}
			fmt.Fprintf(&b, "%s\n", toJson(o))
		}
		cmd.nentry.Incr()
		opipe <- b.String()
	}
}
