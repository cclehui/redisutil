package singleflight

import "sync"

var groupMap sync.Map

func GetGroup(groupKey string) *Group {
	if value, ok := groupMap.Load(groupKey); ok {
		if result, ok2 := value.(*Group); ok2 {
			return result
		}
	}

	newGroup := &Group{}

	groupMap.Store(groupKey, newGroup)

	return newGroup
}
