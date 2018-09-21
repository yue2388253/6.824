package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
	"time"
)

const RQSTINTERVAL = 500 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu		sync.Mutex
	doneChan	chan bool
	valueChan	chan string
	isDone		bool
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.doneChan = make(chan bool)
	ck.valueChan = make(chan string)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	DPrintf("==============ck's Get(key: %v)", key)
	args := GetArgs{Key: key}
	var wg sync.WaitGroup
	num := len(ck.servers)
	for i := range ck.servers {
		wg.Add(1)
		go func(args GetArgs, i int, others int) {
			defer wg.Done()
			for {
				var reply GetReply
				ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
				if ok {
					if reply.Err == OK {
						for j := 0; j < others; j++ {
							ck.doneChan <- true
						}
						ck.valueChan <- reply.Value
						DPrintf("==========ck successfully get key(%v), value: %v.",
							key, reply.Value)
						break
					} else {
						//DPrintf("Not OK.")
					}
				} else {
					DPrintf("Failed to call.")
				}

				select {
				case <- ck.doneChan:
					return
				case <- time.After(RQSTINTERVAL):
					continue
				}
			}
		}(args, i, num)
	}

	// Note: This may receive signal from other operation.
	<- ck.doneChan
	value := <- ck.valueChan
	DPrintf("=========ck get value(%v) from valueChan.", value)

	wg.Wait()
	DPrintf("=============Return from ck's Get.")
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("========ck's PutAppend(key: %v, value: %v, op: %v)==========.",
		key, value, op)
	args := PutAppendArgs{
		Key:	key,
		Value:	value,
		Op: 	op,
	}
	var wg sync.WaitGroup
	num := len(ck.servers)
	for i := range ck.servers {
		wg.Add(1)
		go func(args PutAppendArgs, i int, others int) {
			defer wg.Done()
			for {
				var reply PutAppendReply
				ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
				if ok {
					if reply.Err == OK {
						for j := 0; j < others; j++ {
							ck.doneChan <- true
						}
						DPrintf("=======ck's PutApppend got success from kv[%v].", i)
						break
					} else {
						//DPrintf("Not OK.")
					}
				} else {
					DPrintf("Failed to call.")
				}

				select {
				case <- ck.doneChan:
					DPrintf("This request has been finished by other servers.")
					return
				case <- time.After(RQSTINTERVAL):
					continue
				}
			}
		}(args, i, num)
	}

	<- ck.doneChan
	wg.Wait()
	DPrintf("=============Return from ck's PutAppend.")
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
