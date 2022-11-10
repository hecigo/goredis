package goredis_test

import (
	"context"
	"testing"

	"github.com/hpi-tech/goredis"
	"github.com/hpi-tech/goutils"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

type TestStruct struct {
	Geo Geo `json:"geo"`
}

type Geo struct {
	Loc          string `json:"loc"`
	Unit         string `json:"unit"`
	DistanceType string `json:"distance_type"`
}

func (s *MySuite) SetUpSuite(c *C) {
	goutils.LoadEnv()
	goutils.EnableLogrus()

	// open redis client
	goredis.Open()
}

func (s *MySuite) TearDownSuite(c *C) {
	// close redis client
	goredis.Close()
}

// // Test get various kinds of data from Redis
// func (ms *MySuite) TestGetVariousKinds(c *C) {
// 	// get value from redis
// 	ctxBg := context.Background()
// 	ctx := context.WithValue(context.Background(), goredis.CtxKey_RedisDataType, goredis.SET)

// 	s, err := goredis.Get[string](ctx, "test_string")
// 	c.Assert(err, IsNil)
// 	c.Assert(s, Equals, "string")

// 	i, err := goredis.Get[int](ctxBg, "test_int")
// 	c.Assert(err, IsNil)
// 	c.Assert(i, Equals, 1)

// 	t, err := goredis.Get[time.Time](ctxBg, "test_time")
// 	c.Assert(err, IsNil)
// 	at, _ := goutils.ParseTime("2022-10-26T14:19:08+07:00")
// 	c.Assert(t, Equals, at)

// 	d, err := goredis.Get[time.Duration](ctxBg, "test_duration")
// 	c.Assert(err, IsNil)
// 	c.Assert(d, Equals, time.Second)

// 	j, err := goredis.Get[TestStruct](ctxBg, "test_struct")
// 	c.Assert(err, IsNil)
// 	c.Assert(j, DeepEquals, TestStruct{Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}})
// }

// // Test get hash from Redis
// func (ms *MySuite) TestGetHash(c *C) {
// 	m, err := goredis.Get[map[string]interface{}](context.Background(), "test_hash")
// 	c.Assert(err, IsNil)
// 	c.Assert(m, DeepEquals, map[string]string{"k1": "v1", "k2": "v2"})
// }

// Test get slice from Redis
func (my *MySuite) TestGetSlice(c *C) {
	ctx := context.Background()

	// LIST

	// []string
	ctxList := context.WithValue(ctx, goredis.CtxKey_RedisDataType, goredis.LIST)
	s, err := goredis.Get[[]string](ctxList, "test_slice_list")
	c.Assert(err, IsNil)
	c.Assert(s, DeepEquals, []string{"v1", "v2"})

	// []int
	i, err := goredis.Get[[]int](ctxList, "test_slice_list_int")
	c.Assert(err, IsNil)
	c.Assert(i, DeepEquals, []int{1, 2})

	// multiple keys []int
	mi, err := goredis.Get[[]int](ctxList, "test_slice_list_int", "test_slice_list_int2")
	c.Assert(err, IsNil)
	c.Assert(mi, DeepEquals, map[string]*[]int{"test_slice_list_int": {1, 2}, "test_slice_list_int2": {3, 4}})

	// SET
	ctxSet := context.WithValue(ctx, goredis.CtxKey_RedisDataType, goredis.SET)
	s, err = goredis.Get[[]int](ctxSet, "test_slice_set1")
	c.Assert(err, IsNil)
	c.Assert(s, DeepEquals, []int{1, 2})

	ms, err := goredis.Get[[]int](ctxSet, "test_slice_set1", "test_slice_set2")
	c.Assert(err, IsNil)
	c.Assert(ms, DeepEquals, map[string]*[]int{"test_slice_set1": {1, 2}, "test_slice_set2": {3, 4}})

	// STRING
	s, err = goredis.Get[[]string](ctx, "test_slice_string")
	c.Assert(err, IsNil)
	c.Assert(s, DeepEquals, []string{"v1", "v2"})
}
