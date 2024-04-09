package shardctrler

import (
	"sort"
)

type SCMachine struct {
	me      int
	num     int
	configs []Config
}

func countShard(shards [10]int, gid int) int {
	count := 0
	for _, value := range shards {
		if value == gid {
			count++
		}
	}
	return count
}

func reassign(shards [10]int, groups map[int][]string) [10]int {
	// Deterministic iteration order
	gids := make([]int, 0, len(groups))
	for gid := range groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// No group
	if len(gids) == 0 {
		shards = [10]int{}
		return shards
	}

	// Assign vacant shard to GIDs[0]
	for shard, gid := range shards {
		if gid == 0 {
			shards[shard] = gids[0]
		}
	}

	// Balance
	for {
		maxShard, minShard := 0, 10
		maxShardGid, minShardGid := 0, 0
		for _, gid := range gids {
			shard := countShard(shards, gid)
			if maxShard < shard {
				maxShard = shard
				maxShardGid = gid
			}
			if minShard > shard {
				minShard = shard
				minShardGid = gid
			}
		}

		if maxShard <= minShard+1 {
			return shards
		} else {
			for shard, gid := range shards {
				if gid == maxShardGid {
					shards[shard] = minShardGid
					break
				}
			}
		}
	}
}

func (sc *SCMachine) Join(servers map[int][]string) Err {
	sc.num++

	newShards := sc.configs[sc.num-1].Shards
	newGroups := make(map[int][]string)
	for key, value := range sc.configs[sc.num-1].Groups {
		newGroup := make([]string, len(value))
		copy(newGroup, value)
		newGroups[key] = newGroup
	}

	for key, value := range servers {
		newGroup := make([]string, len(value))
		copy(newGroup, value)
		newGroups[key] = newGroup
	}
	newShards = reassign(newShards, newGroups)

	sc.configs = append(sc.configs, Config{Num: sc.num, Shards: newShards, Groups: newGroups})
	// DPrintf(dCtrler, "M%d Join %v\n", sc.me, sc.configs[sc.num])

	return OK
}

func (sc *SCMachine) Leave(gids []int) Err {
	sc.num++

	newShards := sc.configs[sc.num-1].Shards
	newGroups := make(map[int][]string)
	for key, value := range sc.configs[sc.num-1].Groups {
		newGroup := make([]string, len(value))
		copy(newGroup, value)
		newGroups[key] = newGroup
	}

	for index, value := range newShards {
		for _, gid := range gids {
			if gid == value {
				newShards[index] = 0
				break
			}
		}
	}
	for _, gid := range gids {
		delete(newGroups, gid)
	}
	newShards = reassign(newShards, newGroups)

	sc.configs = append(sc.configs, Config{Num: sc.num, Shards: newShards, Groups: newGroups})
	// DPrintf(dCtrler, "M%d Leave %v\n", sc.me, sc.configs[sc.num])

	return OK
}

func (sc *SCMachine) Move(shard int, gid int) Err {
	sc.num++

	newShards := sc.configs[sc.num-1].Shards
	newGroups := make(map[int][]string)
	for key, value := range sc.configs[sc.num-1].Groups {
		newGroup := make([]string, len(value))
		copy(newGroup, value)
		newGroups[key] = newGroup
	}

	newShards[shard] = gid

	sc.configs = append(sc.configs, Config{Num: sc.num, Shards: newShards, Groups: newGroups})
	// DPrintf(dCtrler, "M%d Move %v\n", sc.me, sc.configs[sc.num])

	return OK
}

func (sc *SCMachine) Query(num int) (Config, Err) {
	if num == -1 || num > sc.num {
		// DPrintf(dCtrler, "M%d Query %v\n", sc.me, sc.configs[sc.num])
		return sc.configs[sc.num], OK
	} else {
		// DPrintf(dCtrler, "M%d Query %v\n", sc.me, sc.configs[num])
		return sc.configs[num], OK
	}
}

func MakeMachine(me int) *SCMachine {
	sc := new(SCMachine)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Num = 0
	sc.configs[0].Shards = [10]int{}
	sc.configs[0].Groups = make(map[int][]string)

	return sc
}
