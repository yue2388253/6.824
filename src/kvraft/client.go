package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	// You will have to modify this struct.
	mu        	sync.Mutex
	id        	int64
	rqstId		int
	leader		int
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
	ck.id = nrand()
	ck.rqstId = 0
	ck.leader = -1
	return ck
}

func CallGet(server *labrpc.ClientEnd, args GetArgs) (bool, string) {
	var reply GetReply
	ok := server.Call("KVServer.Get", &args, &reply)
	if ok && reply.WrongLeader == false {
		DPrintf("Successfully get value of key %v: %v.", args.Key, reply.Value)
		return true, reply.Value
	} else {
		if reply.Err == ErrTimeOut {
			DPrintf("Get Timeout.(%v)", args)
		}
		return false, ""
	}

}

func CallPutAppend(server *labrpc.ClientEnd, args PutAppendArgs) bool {
	var reply PutAppendReply
	ok := server.Call("KVServer.PutAppend", &args, &reply)
	if ok && reply.WrongLeader == false {
		DPrintf("=============Return from ck's PutAppend.")
		return true
	} else {
		if reply.Err == ErrTimeOut {
			DPrintf("PutAppend Timeout.(%v)", args)
		}
		return false
	}

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
	ck.mu.Lock()
	defer ck.mu.Unlock()
	DPrintf("ck's Get(key: %v)", key)
	defer DPrintf("ck's Get finished(key: %v) .", key)
	ck.rqstId++
	args := GetArgs{Key: key, Id: ck.id, ReqId: ck.rqstId}

	if ck.leader != -1 {
		if ok, value := CallGet(ck.servers[ck.leader], args); ok {
			return value
		}
	}
	DPrintf("Cannot connect to the leader.")
	ck.leader = -1

	for {
		for i, server := range ck.servers {
			if ok, value := CallGet(server, args); ok {
				ck.leader = i
				DPrintf("Client finished request.")
				return value
			}
		}
	}
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
	ck.mu.Lock()
	defer ck.mu.Unlock()
	DPrintf("ck's PutAppend(key: %v, value: %v, op: %v)",
		key, value, op)
	defer DPrintf("ck's PutAppend finished.(key: %v, value: %v, op: %v)",
		key, value, op)
	ck.rqstId++
	args := PutAppendArgs{
		Key:	key,
		Value:	value,
		Op: 	op,
		Id:		ck.id,
		ReqId:	ck.rqstId,
	}

	if ck.leader != -1 {
		if CallPutAppend(ck.servers[ck.leader], args) {
			return
		}
	}
	DPrintf("Cannot connect to the leader.")
	ck.leader = -1

	for {
		for i, server := range ck.servers {
			if CallPutAppend(server, args) {
				ck.leader = i
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
