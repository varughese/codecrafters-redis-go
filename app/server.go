package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("NEW CONN", conn)
		go handleRequest(conn)
	}
}

type COMMAND_ID int

const (
	PONG COMMAND_ID = iota
	ECHO
)

type command struct {
	id   COMMAND_ID
	args []byte
}

func handleRequest(conn net.Conn) {
	defer conn.Close()
	for {
		msg, cmd, err := parseConnection(conn)
		fmt.Println(cmd)
		if len(msg) == 0 || err == io.EOF {
			fmt.Println("CLOSING CONN", conn)
			break
		}
		if err != nil {
			fmt.Println("Error reading:", err)
			break
		}

		if cmd.id == ECHO {
			start := []byte("$" + strconv.Itoa(len(msg)) + "\r\n")
			res := append(append(start, msg...), []byte("\r\n")...)
			conn.Write([]byte(res))
		} else if cmd.id == PONG {
			conn.Write([]byte("+PONG\r\n"))
		}

		fmt.Println("READING", msg)
	}
}

func parseConnection(conn net.Conn) ([]byte, *command, error) {
	reader := bufio.NewReader(conn)

	return parseRedisDatatype(reader)
}

func parseRedisDatatype(reader *bufio.Reader) ([]byte, *command, error) {
	dataType, err := reader.ReadByte()
	msg := []byte("")
	cmd := command{id: PONG, args: nil}

	if err != nil {
		return []byte(""), &cmd, err
	}

	msg, err = []byte(""), nil

	switch string(dataType) {
	case "+":
		msg, err = reader.ReadBytes('\n')
	case "-":
		msg, err = reader.ReadBytes('\n')
	case "$":
		l, err := reader.ReadBytes('\n')
		// trim off the \r\n
		stringByteLength, err := strconv.Atoi(string(l[:len(l)-2]))

		if err != nil {
			return msg, &cmd, err
		}

		msg = make([]byte, stringByteLength)
		reader.Read(msg)
	case "*":
		l, err := reader.ReadBytes('\n')
		// trim off the \r\n
		length, err := strconv.Atoi(string(l[:len(l)-2]))

		if err != nil {
			return msg, &cmd, err
		}

		for i := 0; i < length; i++ {
			msg, _, err = parseRedisDatatype(reader)
			if err != nil {
				return msg, &cmd, err
			}
			if i == 1 {
				cmd.id = ECHO
			} else {
				cmd.args = msg
			}
			fmt.Println(string(msg))
			// read til next delimiter
			reader.ReadBytes('\n')
		}
	case "\r":
	case "\n":
		// If the byte is part of a CLRF, return empty
		return msg, &cmd, err
	default:
		err = fmt.Errorf("Invalid start of response. Unknown data type: %s", string(dataType))
	}

	return msg, &cmd, err

}
