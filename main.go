package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func CheckError(err error) {
	if err != nil {
		log.Println("Error: ", err)
		os.Exit(0)
	}
}

type Token struct {
	MsgType string `json:"type"`
	Sender  int    `json:"sender"`
	Origin  int    `json:"origin"`
	Dst     int    `json:"dst"`
	Data    string `json:"data"`
}

type ControlMsg struct {
	Type string `json:"type"`
	Dst  int    `json:"dst"`
	Data string `json:"data"`
}
type Msg struct {
	Type string `json:"type"` // token
	Data string `json:"data"`
}

type ActiveMonitorFrame struct {
	ActiveMonitor int `json:"am"`
}

type Polls struct {
	No int `json:"No"`
}

func UpdateData(data string) string {

	num, _ := strconv.Atoi(data)
	num = num + 1
	return strconv.Itoa(num)
}

func (t Token) IsEmpty() bool {
	return len(t.Data) == 0 && t.MsgType == "none"
}
func GetCount(mp map[int]bool) int {
	res := 0
	for _, v := range mp {
		if v {
			res = res + 1
		}
	}
	return res
}

const UDP_PACKET_SIZE = 1024

func RepackMessage(msg []byte, id int, from int) []byte {
	message := Msg{}
	err := json.Unmarshal(msg, &message)
	CheckError(err)
	if message.Type == "Token" {
		tokenMsg := Token{}
		err = json.Unmarshal([]byte(message.Data), &tokenMsg)
		CheckError(err)
		tokenMsg.Dst = id
		tokenMsg.Sender = from
		sndBuf, _ := json.Marshal(&tokenMsg)
		message.Data = string(sndBuf)
	} else {
		return msg
	}
	buff, _ := json.Marshal(message)
	return buff
}

func GetNbgConn(LocalAddr *net.UDPAddr, id int, n int) (*net.UDPConn, int) {
	var remConn *net.UDPConn
	nbg := (id + 1) % n
	RemoteAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(30000+nbg))
	CheckError(err)
	remConn, err = net.DialUDP("udp", LocalAddr, RemoteAddr)

	return remConn, nbg
}

type Message struct {
	data     string
	dst      int
	accepted bool
}

//polls
func ServiceConnMgr(id int, ServiceConn *net.UDPConn, messages *[]Message, msgMtx *sync.Mutex, isPortEnabled *uint32, needToDropToken *uint32 /*, needToChangePortState *uint32*/) {
	flag := false
	for !flag {
		buff := make([]byte, 1024)
		n, _, err := ServiceConn.ReadFromUDP(buff)
		CheckError(err)
		fmt.Println("node ", id, ": recieved service message:\\ \n", string(buff[0:n]))
		controlMessage := ControlMsg{}
		err = json.Unmarshal(buff[0:n], &controlMessage)
		switch controlMessage.Type {
		case "send":
			msgMtx.Lock()
			*messages = append(*messages, Message{
				data:     controlMessage.Data,
				dst:      controlMessage.Dst,
				accepted: false})
			msgMtx.Unlock()
			/*
				case "terminate":
					if atomic.LoadUint32(isPortEnabled) == 1 {
						//needToChangePortState = true
						atomic.CompareAndSwapUint32(needToChangePortState, 0, 1)
					}
				case "recover":
					{
						if atomic.LoadUint32(isPortEnabled) == 0 {
							atomic.CompareAndSwapUint32(needToChangePortState, 0, 1)
						}
					}
			*/
		case "drop":
			atomic.CompareAndSwapUint32(needToDropToken, 0, 1)
		}
	}
}

//send
func ReNode(id int, t int, numP int, startWg *sync.WaitGroup, finalWg *sync.WaitGroup) {
	log.Println("Hello from ", id, "n==", numP)
	ServerAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(30000+id))
	CheckError(err)
	var ServiceAddr *net.UDPAddr
	ServiceAddr, err = net.ResolveUDPAddr("udp", ":"+strconv.Itoa(40000+id))
	CheckError(err)
	var LocalAddr *net.UDPAddr
	LocalAddr, err = net.ResolveUDPAddr("udp", ":0")
	CheckError(err)
	timeout := 100
	timeout += 10 * t
	var TokenConn *net.UDPConn
	TokenConn, err = net.ListenUDP("udp", ServerAddr)
	CheckError(err)
	var ServiceConn *net.UDPConn
	ServiceConn, err = net.ListenUDP("udp", ServiceAddr)
	CheckError(err)
	defer ServiceConn.Close()

	var isPortEnabled uint32
	isPortEnabled = 1
	/*var needToChangePortState uint32
	needToChangePortState = 0*/
	var needToDropToken uint32
	needToDropToken = 0

	buff := make([]byte, 1024)

	am := -1
	messages := make([]Message, 0)
	isActivePolls := false
	needToReproduceToken := false
	flag := false

	var msgMtx = &sync.Mutex{}

	finalWg.Add(1)
	log.Println("Waiting for startwg")
	startWg.Wait()
	log.Println("Start received")

	log.Println("id=", id, "Launching other routine")

	go ServiceConnMgr(id, ServiceConn, &messages, msgMtx, &isPortEnabled, &needToDropToken /*, &needToChangePortState*/)

	for !flag {
		/*if atomic.LoadUint32(&needToChangePortState) == 1 {
			if atomic.LoadUint32(&isPortEnabled) == 1 {
				TokenConn.Close()
				atomic.StoreUint32(&isPortEnabled, 0)
			} else {
				TokenConn, err = net.ListenUDP("udp", ServerAddr)
				CheckError(err)
				atomic.StoreUint32(&isPortEnabled, 1)
			}
		}*/

		if atomic.LoadUint32(&isPortEnabled) == 1 {
			TokenConn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(timeout)))
			n, _, err := TokenConn.ReadFromUDP(buff)
			nerr, ok := err.(net.Error)
			if ok && nerr.Timeout() {
				log.Println("id=", id, "TIMEOUT")
				//lets initiate polls
				pollsMsg := Polls{id}
				pollsBuf, _ := json.Marshal(pollsMsg)
				message := Msg{"Polls", string(pollsBuf)}
				msgBuf, _ := json.Marshal(message)
				remConn, _ := GetNbgConn(LocalAddr, id, numP)
				_, err := remConn.Write(msgBuf)
				CheckError(err)
				continue
			}

			if 1 == atomic.LoadUint32(&needToDropToken) {
				atomic.StoreUint32(&needToDropToken, 1)
				continue
			}
			message := Msg{}
			err = json.Unmarshal(buff[0:n], &message)
			CheckError(err)
			needToSendMessage := true
			isToken := false
			oldToken := Token{}
			switch message.Type {
			case "Polls":
				{
					log.Println("id=", id, "Polls received isActivePolls=", isActivePolls)
					pollMsg := Polls{}
					err = json.Unmarshal([]byte(message.Data), &pollMsg)
					CheckError(err)

					if id >= pollMsg.No {
						pollMsg.No = id
						isActivePolls = !isActivePolls
					}
					needToSendMessage = isActivePolls
					if !isActivePolls {
						am = pollMsg.No
					} else {
						buff, _ := json.Marshal(pollMsg)
						message.Data = string(buff)
					}

					log.Println("id=", id, "Polls processed am=", am, "isActivePolls=", isActivePolls)
					needToReproduceToken = (am == id)
					needToSendMessage = needToReproduceToken || needToSendMessage
				}
			case "ActiveFrame":
				{
					//nothing to do here
					needToSendMessage = false
				}
			case "Token":
				{
					isToken = true

					needToSendMessage = 0 == atomic.LoadUint32(&needToDropToken) && !isActivePolls

					tokenMsg := Token{}
					err = json.Unmarshal([]byte(message.Data), &tokenMsg)
					oldToken = tokenMsg
					if 0 == atomic.LoadUint32(&needToDropToken) && !isActivePolls {
						if tokenMsg.IsEmpty() {
							msgMtx.Lock()
							if len(messages) > 0 {
								curMsg := messages[len(messages)-1]
								tokenMsg = Token{"message", id, id, curMsg.dst, curMsg.data}
							}
							msgMtx.Unlock()
						}
						if tokenMsg.Dst == id {
							switch tokenMsg.MsgType {
							case "notification":
								{
									i := 0
									msg := Message{}
									found := false
									msgMtx.Lock()
									for i, msg = range messages {
										if msg.data == tokenMsg.Data && msg.dst == tokenMsg.Origin {
											found = true
											break
										}
									}

									if found {
										messages = append(messages[:i], messages[i+1:]...)
										tokenMsg = Token{"none", id, id, -1, ""}
									}
									msgMtx.Unlock()
								}
							case "message":
								{
									tokenMsg.Dst = tokenMsg.Origin
									tokenMsg.Origin = id
									tokenMsg.MsgType = "notification"
								}
							}

						}
						tokenMsg.Sender = id
						tbuff, _ := json.Marshal(tokenMsg)
						message.Data = string(tbuff)
						time.Sleep(time.Millisecond * time.Duration(t))
					}
					atomic.StoreUint32(&needToDropToken, 1)
				}
			}
			if !needToSendMessage {
				continue
			}

			RemoteConn, nbg := GetNbgConn(LocalAddr, id, numP)
			if isToken && oldToken.IsEmpty() {
				fmt.Println("node ", id, ": recieved token from node ", oldToken.Sender, " sending token to node ", nbg)
			} else if isToken {
				if oldToken.MsgType == "message" {
					fmt.Println("node ", id, ": recieved token from node ", oldToken.Sender, "with data from node ", oldToken.Origin, "\\ \n(data='", oldToken.Data, "') sending token to node ", nbg)
				} else {

					fmt.Println("node ", id, ": recieved token from node ", oldToken.Sender, "with delivery notification from node ", oldToken.Origin, " sending token to node ", nbg)
				}
			}
			if needToReproduceToken {
				tokenMsg := Token{"none", id, id, -1, ""}
				tokenBuff, _ := json.Marshal(tokenMsg)
				message.Type = "Token"
				message.Data = string(tokenBuff)
				needToReproduceToken = false
			}

			if needToSendMessage {
				msgBuff, _ := json.Marshal(&message)
				if nbg != (id+1)%n {
					msgBuff = RepackMessage(msgBuff, nbg, id)
				}
				_, err := RemoteConn.Write(msgBuff)
				CheckError(err)
				RemoteConn.Close()
			}
		}

	}
	flag = true
	finalWg.Done()

}

func main() {
	//rand.Seed(0)
	rand.Seed(time.Now().UTC().UnixNano())
	nPtr := flag.Int("n", 3, "number of nodes")
	tPtr := flag.Int("t", 2, "time")
	flag.Parse()
	n := *nPtr
	t := *tPtr

	fmt.Println("n==", n, "t==", t)
	var finalWg sync.WaitGroup
	var startWg sync.WaitGroup

	startWg.Add(1)
	for i := 0; i < n; i++ {
		log.Println(i)
		go ReNode(i, t, n, &startWg, &finalWg)
	}
	log.Println("Releasing start wg")
	startWg.Done()
	flag := false
	for !flag {
		finalWg.Wait()
	}

}
