package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation	string
	Key			string
	Value		string
	Id			int64
	ReqId		int
}

type Rqst struct {
	mu    sync.Mutex
	index int
	oper  Op
	ch    chan bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyValues	map[string]string
	rqst		Rqst
	chanRqst	chan Op
	chanDone	chan bool
	chanFinished	chan bool
	ack			map[int64]int

}

// Return true if this op is duplicated.
func (kv *KVServer) CheckDup(op Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	id := op.Id
	reqId := op.ReqId
	v, ok := kv.ack[id]
	if ok {
		return v >= reqId
	}
	return false
}

func (kv *KVServer) AppendEntryToLog(oper Op) bool {
	if _, leader := kv.rf.GetState(); leader {
		DPrintf("kv[%v] send oper(%v) to chanRqst.", kv.me, oper)
		kv.chanRqst <- oper
		return true
	} else {
		return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("kv[%v].Get(args: %v)", kv.me, args)

	oper := Op{
		Operation: 	OperGet,
		Key:       	args.Key,
		Id:			args.Id,
		ReqId: 		args.ReqId,
	}

	if !kv.AppendEntryToLog(oper) {
		reply.WrongLeader = true
		reply.Err = ErrNoKey
		return
	}

	defer func() {
		kv.rqst.mu.Lock()
		kv.rqst.index = -1
		kv.rqst.mu.Unlock()
		kv.chanFinished <- true
		DPrintf("kv[%v].Get finished.(args: %v)", kv.me, args)
	}()

	if ok := <- kv.chanDone; ok {
		reply.WrongLeader = false
		reply.Err = OK
		if value, ok := kv.keyValues[args.Key]; ok {
			reply.Value = value
			DPrintf("============Return OK.")
			return
		} else {
			reply.Err = ErrNoKey
			DPrintf("Error: No key.")
			return
		}
	} else {
		reply.WrongLeader = true
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("kv[%v].PutAppend(args: %v)", kv.me, args)

	oper := Op{
		Operation:	args.Op,
		Key:		args.Key,
		Value: 		args.Value,
		Id:			args.Id,
		ReqId: 		args.ReqId,
	}

	if !kv.AppendEntryToLog(oper) {
		reply.WrongLeader = true
		return
	}

	defer func() {
		kv.rqst.mu.Lock()
		kv.rqst.index = -1
		kv.rqst.mu.Unlock()
		kv.chanFinished <- true
		DPrintf("kv[%v].PutAppend finished.(args: %v)", kv.me, args)
	}()

	if ok := <- kv.chanDone; ok {
		reply.WrongLeader = false
		reply.Err = OK
		DPrintf("============Return OK.")
		return
	} else {
		reply.WrongLeader = true
		return
	}
}

func (kv *KVServer) Apply(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op.Operation {
	case OperAppend:
		kv.keyValues[op.Key] += op.Value
		DPrintf("Now kv[%v]'s kvs is %v", kv.me, kv.keyValues)
	case OperPut:
		kv.keyValues[op.Key] = op.Value
		DPrintf("Now kv[%v]'s kvs is %v", kv.me, kv.keyValues)
	}
	kv.ack[op.Id] = op.ReqId
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.keyValues = make(map[string]string)
	kv.rqst = Rqst{index: -1}
	kv.rqst.ch = make(chan bool)
	kv.chanRqst = make(chan Op)
	kv.chanDone = make(chan bool)
	kv.chanFinished = make(chan bool)
	kv.ack = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for {
			select {
			case cmd := <- kv.applyCh:
				DPrintf("kv[%v] received msg from applyCh", kv.me)
				if op, ok := (cmd.Command).(Op); ok {
					if !kv.CheckDup(op) {
						kv.Apply(op)
						DPrintf("Fresh request.")
					} else {
						DPrintf("Duplicated request: %v.", op)
					}

					// determine whether the received msg is the reply of the request.
					kv.rqst.mu.Lock()
					if cmd.CommandIndex == kv.rqst.index &&
						op == kv.rqst.oper {
							DPrintf("Finished msg")
							kv.rqst.ch <- true
					}
					kv.rqst.mu.Unlock()

				} else {
					panic(ok)
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case oper := <- kv.chanRqst:
				if index, _, isLeader := kv.rf.Start(oper); isLeader {
					DPrintf("kv[%v] received rqst(%v).", kv.me, oper)
					kv.rqst.mu.Lock()
					kv.rqst.index = index
					kv.rqst.oper = oper
					kv.rqst.mu.Unlock()

					select {
					case <- kv.rqst.ch:
						kv.chanDone <- true
						DPrintf("kv[%v] done(Op: %v).", kv.me, oper)
					case <- time.After(1 * time.Second):
						kv.chanDone <- false
						DPrintf("kv[%v] timeout(Op: %v).", kv.me, oper)
					}

				} else {
					kv.chanDone <- false
				}
			}

			// Ensure that this server can only handle an operation at a time.
			DPrintf("kv[%v] block until the request return.", kv.me)
			<- kv.chanFinished
			DPrintf("kv[%v] request has returned.", kv.me)

		}
	}()

	return kv
}
