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
	PING COMMAND_ID = iota
	ECHO
)

type command struct {
	id   COMMAND_ID
	args []byte
}

func (cmd *command) run(conn net.Conn) {
	if cmd.id == ECHO {
		echo(cmd, conn)
	}
	if cmd.id == PING {
		ping(cmd, conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()
	for {
		cmd, err := parseConnection(conn)
		if cmd == nil || err == io.EOF {
			fmt.Println("CLOSING CONN", conn)
			break
		}
		if err != nil {
			fmt.Println("Error reading:", err)
			break
		}

		fmt.Println("GOT CMD", cmd)

		cmd.run(conn)
	}
}

func parseConnection(conn net.Conn) (*command, error) {
	reader := bufio.NewReader(conn)
	rawRedisData, err := parseRedisDatatype(reader)
	return parseRedisCommand(rawRedisData), err
}

func parseRedisCommand(rawRedisData *redisData) *command {
	cmd := command{id: PING}

	if len(rawRedisData.array) < 1 {
		return &cmd
	}
	commandString := rawRedisData.array[0].bulkString
	args := rawRedisData.array[1].bulkString

	switch string(commandString) {
	case "ECHO":
		cmd.id = ECHO
		cmd.args = args
	default:
		cmd.id = PING
	}

	return &cmd
}

type redisData struct {
	simpleString []byte
	errorString  []byte
	bulkString   []byte
	integer      int
	array        []redisData
}

func parseRedisDatatype(reader *bufio.Reader) (*redisData, error) {
	dataType, err := reader.ReadByte()
	msg := []byte("")
	data := redisData{}

	if err != nil {
		return &data, err
	}

	msg, err = []byte(""), nil

	switch string(dataType) {
	case "+":
		msg, err = reader.ReadBytes('\n')
		data.simpleString = msg
	case "-":
		msg, err = reader.ReadBytes('\n')
		data.errorString = msg
	case "$":
		l, err := reader.ReadBytes('\n')
		// trim off the \r\n
		stringByteLength, err := strconv.Atoi(string(l[:len(l)-2]))

		if err != nil {
			return &data, err
		}

		msg = make([]byte, stringByteLength)
		reader.Read(msg)
		data.bulkString = msg
	case "*":
		l, err := reader.ReadBytes('\n')
		// trim off the \r\n
		length, err := strconv.Atoi(string(l[:len(l)-2]))

		if err != nil {
			return &data, err
		}

		var resultArray []redisData = make([]redisData, length)
		for i := 0; i < length; i++ {
			redisData, err := parseRedisDatatype(reader)
			if err != nil {
				return redisData, err
			}
			resultArray[i] = *redisData

			// Read til next delimiter
			reader.ReadBytes('\n')
		}
		data.array = resultArray
	case "\r":
	case "\n":
		// If the byte is part of a CLRF, return empty
		return &data, err
	default:
		err = nil
		// err = fmt.Errorf("Invalid start of response. Unknown data type: %s", string(dataType))
	}

	return &data, err

}

func serialize(str []byte) []byte {
	start := []byte("$" + strconv.Itoa(len(str)) + "\r\n")
	res := append(append(start, str...), []byte("\r\n")...)
	return res
}

func echo(cmd *command, conn net.Conn) {
	conn.Write(serialize(cmd.args))
}

func ping(cmd *command, conn net.Conn) {
	conn.Write([]byte("+PONG\r\n"))
}
