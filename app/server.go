package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"
)

type entry struct {
	data       []byte
	expiryTime time.Time
	hasExpiry  bool
}

var DATABASE map[string]entry

func main() {
	DATABASE = make(map[string]entry)

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
	SET
	GET
)

type command struct {
	id   COMMAND_ID
	args []redisData
}

func (cmd *command) run(conn net.Conn) {
	if cmd.id == ECHO {
		echo(cmd, conn)
	}
	if cmd.id == PING {
		ping(cmd, conn)
	}
	if cmd.id == SET {
		set(cmd, conn)
	}
	if cmd.id == GET {
		get(cmd, conn)
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
	cmd.args = rawRedisData.array[1:]

	switch string(bytes.ToUpper(commandString)) {
	case "ECHO":
		cmd.id = ECHO
	case "SET":
		cmd.id = SET
	case "GET":
		cmd.id = GET
	case "PING":
		cmd.id = PING
	default:
		cmd = command{}
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
	case ":":
		msg, err = reader.ReadBytes('\n')
		parsedInt, err := strconv.Atoi(string(msg[:len(msg)-2]))
		if err != nil {
			err = fmt.Errorf("Failed to parse int from %s", msg)
			return &data, err
		}
		data.integer = parsedInt
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
		err = fmt.Errorf("Invalid start of response. Unknown data type: %s", string(dataType))
	}

	return &data, err

}

func serialize(str []byte) []byte {
	if str == nil {
		// NULL in RESP:
		return []byte("$-1\r\n")
	}
	start := []byte("$" + strconv.Itoa(len(str)) + "\r\n")
	res := append(append(start, str...), []byte("\r\n")...)
	return res
}

func echo(cmd *command, conn net.Conn) {
	conn.Write(serialize(cmd.args[0].bulkString))
}

func ping(cmd *command, conn net.Conn) {
	conn.Write([]byte("+PONG\r\n"))
}

func set(cmd *command, conn net.Conn) {
	key := cmd.args[0].bulkString
	value := cmd.args[1].bulkString
	hasExpiry := len(cmd.args) == 4
	entry := entry{
		data:      value,
		hasExpiry: hasExpiry,
	}
	if hasExpiry {
		expiryTimeMilliseconds, _ := strconv.Atoi(string(cmd.args[3].bulkString))
		entry.expiryTime = time.Now().Add(time.Millisecond * time.Duration(expiryTimeMilliseconds))
		fmt.Println("ms:", expiryTimeMilliseconds)
		fmt.Println("Curren time", time.Now())
		fmt.Println("Expire at  ", entry.expiryTime)
	}
	DATABASE[string(key)] = entry
	fmt.Println("Inserted", value, "at ", string(key))
	conn.Write([]byte("+OK\r\n"))
}

func get(cmd *command, conn net.Conn) {
	currentTime := time.Now()
	key := cmd.args[0].bulkString
	entry := DATABASE[string(key)]
	value := entry.data
	if entry.hasExpiry {
		fmt.Println("Curren time", time.Now())
		fmt.Println("Expires at ", entry.expiryTime)
		if entry.expiryTime.Before(currentTime) {
			value = nil
			delete(DATABASE, string(key))
		}
	}
	conn.Write(serialize(value))
}
