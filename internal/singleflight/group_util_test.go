package singleflight

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient_Get(t *testing.T) {

	groupKey := "TestClient_Get"
	group := GetGroup(groupKey)

	assert.Equal(t, group, GetGroup(groupKey))
}
