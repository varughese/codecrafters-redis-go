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

func handleRequest(conn net.Conn) {
	for {
		// ReadString reads the data up until a new line.
		// If there is no new line, it sets err to io.EOF.
		// In this case, then we close the connection.
		data, err := bufio.NewReader(conn).ReadString('\n')
		fmt.Println("READING", data)
		if err != nil {
			if len(data) == 0 && err == io.EOF {
				fmt.Println("CLOSING CONN", conn)
				conn.Close()
				break
			}
			fmt.Println("Error reading:", err.Error())
		}

		conn.Write([]byte("+PONG\r\n"))
	}
}
