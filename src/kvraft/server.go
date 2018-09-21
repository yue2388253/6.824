package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1

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
}

type Rqst struct {
	mu					sync.Mutex
	cond				*sync.Cond
	currentRqstIndex	int
	isDone				bool
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

}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("kv[%v].Get(args: %v)", kv.me, args)

	reply.WrongLeader = true
	reply.Err = ErrNoKey

	oper := Op{
		Operation:	OPER_GET,
		Key:		args.Key,
	}
	index, _, isLeader := kv.rf.Start(oper)

	if !isLeader {
		//DPrintf("Server[%v]'s Get return false.", kv.me)
		return
	}

	DPrintf("============kv[%v] is the leader.", kv.me)

	kv.rqst.mu.Lock()
	kv.rqst.currentRqstIndex = index
	kv.rqst.isDone = false
	kv.rqst.mu.Unlock()
	defer func() {
		kv.rqst.mu.Lock()
		kv.rqst.isDone = false
		kv.rqst.currentRqstIndex = -1
		kv.rqst.mu.Unlock()
	}()

	kv.rqst.mu.Lock()
	for !kv.rqst.isDone {
		kv.rqst.cond.Wait()
	}
	kv.rqst.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK
	if value, ok := kv.keyValues[args.Key]; ok {
		reply.Value = value
		return
	} else {
		reply.Err = ErrNoKey
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// TODO: add a locker or a queue so that a server can only handle one request at a time.
	DPrintf("kv[%v].PutAppend(args: %v)", kv.me, args)

	reply.WrongLeader = true
	reply.Err = ErrNoKey

	oper := Op{
		Operation:	args.Op,
		Key:		args.Key,
		Value: 		args.Value,
	}
	index, _, isLeader := kv.rf.Start(oper)

	if !isLeader {
		//DPrintf("Server[%v]'s PutAppend return false.", kv.me)
		return
	}

	DPrintf("============kv[%v] is the leader.", kv.me)

	kv.rqst.mu.Lock()
	kv.rqst.currentRqstIndex = index
	kv.rqst.isDone = false
	kv.rqst.mu.Unlock()
	defer func() {
		kv.rqst.mu.Lock()
		kv.rqst.isDone = false
		kv.rqst.currentRqstIndex = -1
		kv.rqst.mu.Unlock()
	}()

	kv.rqst.mu.Lock()
	DPrintf("Waiting for server applied cmd.")
	for !kv.rqst.isDone {
		kv.rqst.cond.Wait()
	}
	kv.rqst.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK
	DPrintf("============Return OK.")
	return
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
	kv.rqst = Rqst{currentRqstIndex: -1, isDone: false}
	kv.rqst.cond = sync.NewCond(&kv.rqst.mu)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	DPrintf("StartKVServer(%v).", me)

	go func() {
		for {
			select {
			case cmd := <- kv.applyCh:
				kv.mu.Lock()
				DPrintf("kv[%v] received msg from applyCh", kv.me)
				if op, ok := (cmd.Command).(Op); ok {
					switch op.Operation {
					case OPER_GET:
						DPrintf("kv[%v] received Get from applyCh.", kv.me)
					case OPER_APPEND:
						DPrintf("kv[%v] received Append from applyCh.", kv.me)
						if oldValue, ok := kv.keyValues[op.Key]; ok {
							newValue := oldValue + op.Value
							kv.keyValues[op.Key] = newValue
							DPrintf("Now kv[%v]'s kvs is %v", kv.me, kv.keyValues)
						} else {
							panic(ok)
						}
					case OPER_PUT:
						DPrintf("kv[%v] received Put from applyCh.", kv.me)
						kv.keyValues[op.Key] = op.Value
						DPrintf("Now kv[%v]'s kvs is %v", kv.me, kv.keyValues)
					}

					// determine whether the received msg is the reply of the request.
					kv.rqst.mu.Lock()
					if cmd.CommandIndex == kv.rqst.currentRqstIndex {
						DPrintf("Finished msg")
						kv.rqst.isDone = true
						kv.rqst.cond.Signal()
					}
					kv.rqst.mu.Unlock()

				} else {
					panic(ok)
				}
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
