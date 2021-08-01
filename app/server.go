package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
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
	id      COMMAND_ID
	content []byte
}

func handleRequest(conn net.Conn) {
	defer conn.Close()
	for {
		msg, err := parseConnection(conn)
		if len(msg) == 0 || err != nil {
			if err == io.EOF {
				fmt.Println("CLOSING CONN", conn)
				break
			}
			fmt.Println("Error reading:", err.Error())
			break
		}

		fmt.Println("READING", msg)
		conn.Write([]byte("+PONG\r\n"))
	}
}

func parseConnection(conn net.Conn) ([]byte, error) {
	reader := bufio.NewReader(conn)
	dataType, err := reader.ReadByte()
	msg := []byte("")

	if err != nil {
		return []byte(""), err
	}

	msg, err = []byte(""), nil

	switch string(dataType) {
	case "+":
		msg, err = reader.ReadBytes('\n')
	case "-":
		msg, err = reader.ReadBytes('\n')
	case "*":
		msg, err = reader.ReadBytes('\n')
	default:
		err = fmt.Errorf("Invalid start of response. Unknown data type: %b", dataType)
	}

	return msg, err
}
