package kvraft

type KVMachine struct {
	me    int
	state map[string]string
}

func (kv *KVMachine) Get(key string) (string, Err) {
	value, ok := kv.state[key]
	if ok {
		DPrintf(dMachine, "M%d Get {%v:%v}\n", kv.me, key, value)
		return value, OK
	} else {
		DPrintf(dMachine, "M%d Get no %v\n", kv.me, key)
		return "", ErrNoKey
	}
}

func (kv *KVMachine) Put(key string, value string) (string, Err) {
	kv.state[key] = value
	DPrintf(dMachine, "M%d Put {%v:%v}\n", kv.me, key, value)
	return "", OK
}

func (kv *KVMachine) Append(key string, value string) (string, Err) {
	kv.state[key] += value
	DPrintf(dMachine, "M%d Append {%v:%v}\n", kv.me, key, value)
	return "", OK
}

func (kv *KVMachine) SetState(state map[string]string) {
	kv.state = state
	//r := bytes.NewBuffer(data)
	//d := labgob.NewDecoder(r)
	//var state map[string]string
	//if d.Decode(&state) != nil {
	//
	//} else {
	//	kv.state = state
	//}
}

func (kv *KVMachine) GetState() map[string]string {
	return kv.state
	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(kv.state)
	//snapshot := w.Bytes()
	//return snapshot
}

func MakeMachine(me int) *KVMachine {
	kv := new(KVMachine)
	kv.me = me
	kv.state = make(map[string]string)

	return kv
}
