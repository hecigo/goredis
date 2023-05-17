package goredis_test

import (
	"context"
	"fmt"
	"time"

	"github.com/hecigo/goredis"
	"github.com/hecigo/goutils"
	. "gopkg.in/check.v1"
)

type HandlerSuite struct{}

var _ = Suite(&HandlerSuite{})

type TestStruct struct {
	Geo Geo `json:"geo"`
}

type Geo struct {
	Loc          string `json:"loc"`
	Unit         string `json:"unit"`
	DistanceType string `json:"distance_type"`
}

func (s *HandlerSuite) SetUpSuite(c *C) {
	fmt.Println("SetUpSuite > HandlerSuite")
	goutils.QuickLoad()
	goredis.Open()
}

func (s *HandlerSuite) TearDownSuite(c *C) {
	// close redis client
	goredis.Close()
}

// Test set/get various kinds of data from Redis
func (ms *HandlerSuite) TestVariousKinds(c *C) {
	// get value from redis
	ctx := context.Background()

	err := goredis.Set(ctx, "test_string", "string")
	c.Assert(err, IsNil)
	s, err := goredis.Get[string](ctx, "test_string")
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "string")

	err = goredis.Set(ctx, "test_int", 1)
	c.Assert(err, IsNil)
	i, err := goredis.Get[int](ctx, "test_int")
	c.Assert(err, IsNil)
	c.Assert(i, Equals, 1)

	now := goutils.Now()
	err = goredis.Set(ctx, "test_time", now)
	c.Assert(err, IsNil)
	t, err := goredis.Get[time.Time](ctx, "test_time")
	c.Assert(err, IsNil)
	c.Assert(t, DeepEquals, now)

	err = goredis.Set(ctx, "test_duration", time.Second)
	c.Assert(err, IsNil)
	d, err := goredis.Get[time.Duration](ctx, "test_duration")
	c.Assert(err, IsNil)
	c.Assert(d, Equals, time.Second)

	err = goredis.Set(ctx, "test_struct", TestStruct{Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}})
	c.Assert(err, IsNil)
	j, err := goredis.Get[TestStruct](ctx, "test_struct")
	c.Assert(err, IsNil)
	c.Assert(j, DeepEquals, TestStruct{Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}})

	// get multiple struct
	temp := TestStruct{Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}}
	err = goredis.MSet(ctx, map[string]interface{}{
		"test_struct":  temp,
		"test_struct2": temp,
	})
	c.Assert(err, IsNil)
	j2, err := goredis.Get[TestStruct](ctx, "test_struct", "test_struct2")
	c.Assert(err, IsNil)
	c.Assert(j2, DeepEquals, map[string]*TestStruct{
		"test_struct":  {Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}},
		"test_struct2": {Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}},
	})
}

// Test set/get hash from Redis
func (ms *HandlerSuite) TestHash(c *C) {
	ctx := context.WithValue(context.Background(), goredis.CtxKey_DataType, goredis.HASH)

	// map[string]string
	err := goredis.Set(ctx, "test_hash", map[string]string{"k1": "v1", "k2": "v2"})
	c.Assert(err, IsNil)
	m, err := goredis.Get[map[string]string](ctx, "test_hash")
	c.Assert(err, IsNil)
	c.Assert(m, DeepEquals, map[string]string{"k1": "v1", "k2": "v2"})

	// struct
	err = goredis.Set(ctx, "test_hash_struct", TestStruct{Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}})
	c.Assert(err, IsNil)
	m2, err := goredis.Get[TestStruct](ctx, "test_hash_struct")
	c.Assert(err, IsNil)
	c.Assert(m2, DeepEquals, TestStruct{Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}})

	// multiple keys of map[string]string
	temp := map[string]string{"k1": "v1", "k2": "v2"}
	err = goredis.MSet(ctx, map[string]interface{}{
		"test_hash":  temp,
		"test_hash2": temp,
	})
	c.Assert(err, IsNil)
	mm, err := goredis.Get[map[string]string](ctx, "test_hash", "test_hash2")
	c.Assert(err, IsNil)
	c.Assert(mm, DeepEquals, map[string]*map[string]string{"test_hash": {"k1": "v1", "k2": "v2"}, "test_hash2": {"k1": "v1", "k2": "v2"}})

	// multiple keys of map[string]struct
	tempStruct := map[string]TestStruct{
		"geo": {Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}},
	}
	err = goredis.MSet(ctx, map[string]interface{}{
		"test_hash_struct":  tempStruct,
		"test_hash_struct2": tempStruct,
	})
	c.Assert(err, IsNil)
	mm2, err := goredis.Get[map[string]TestStruct](ctx, "test_hash_struct", "test_hash_struct2")
	c.Assert(err, IsNil)
	c.Assert(mm2, DeepEquals, map[string]*map[string]TestStruct{
		"test_hash_struct":  &tempStruct,
		"test_hash_struct2": &tempStruct,
	})

	// map[string]int
	mi, err := goredis.Get[map[string]int](ctx, "test_hash_int")
	c.Assert(err, IsNil)
	c.Assert(mi, DeepEquals, map[string]int{"k1": 1, "k2": 2})

	// multiple keys of map[string]int
	mmi, err := goredis.Get[map[string]int](ctx, "test_hash_int", "test_hash_int2")
	c.Assert(err, IsNil)
	c.Assert(mmi, DeepEquals, map[string]*map[string]int{"test_hash_int": {"k1": 1, "k2": 2}, "test_hash_int2": {"k3": 3, "k4": 4}})

	// struct
	ctxHash := context.WithValue(ctx, goredis.CtxKey_DataType, goredis.HASH)
	g, err := goredis.Get[Geo](ctxHash, "test_hash_struct3")
	c.Assert(err, IsNil)
	c.Assert(g, DeepEquals, Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"})

	// multiple keys of struct
	mg, err := goredis.Get[Geo](ctxHash, "test_hash_struct3", "test_hash_struct4")
	c.Assert(err, IsNil)
	c.Assert(mg, DeepEquals, map[string]*Geo{
		"test_hash_struct3": {Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"},
		"test_hash_struct4": {Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"},
	})
}

// Test get slice from Redis
func (my *HandlerSuite) TestGetSlice(c *C) {
	ctx := context.Background()

	// LIST

	// []string
	ctxList := context.WithValue(ctx, goredis.CtxKey_DataType, goredis.LIST)
	err := goredis.Set(ctxList, "test_slice_list", []string{"v1", "v2"})
	c.Assert(err, IsNil)
	s, err := goredis.Get[[]string](ctxList, "test_slice_list")
	c.Assert(err, IsNil)
	c.Assert(s, DeepEquals, []string{"v1", "v2"})

	// []int
	err = goredis.Set(ctxList, "test_slice_list_int", []int{1, 2})
	c.Assert(err, IsNil)
	i, err := goredis.Get[[]int](ctxList, "test_slice_list_int")
	c.Assert(err, IsNil)
	c.Assert(i, DeepEquals, []int{1, 2})

	// multiple keys []int
	err = goredis.MSet(ctxList, map[string]interface{}{
		"test_slice_list_int":  []int{1, 2},
		"test_slice_list_int2": []int{3, 4},
	})
	c.Assert(err, IsNil)
	mi, err := goredis.Get[[]int](ctxList, "test_slice_list_int", "test_slice_list_int2")
	c.Assert(err, IsNil)
	c.Assert(mi, DeepEquals, map[string]*[]int{"test_slice_list_int": {1, 2}, "test_slice_list_int2": {3, 4}})

	// []struct
	j, err := goredis.Get[[]TestStruct](ctxList, "test_slice_list_struct1")
	c.Assert(err, IsNil)
	c.Assert(j, DeepEquals, []TestStruct{{Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}}, {Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}}})

	// []struct with multiple keys
	mj, err := goredis.Get[[]TestStruct](ctxList, "test_slice_list_struct1", "test_slice_list_struct2")
	c.Assert(err, IsNil)
	c.Assert(mj, DeepEquals, map[string]*[]TestStruct{
		"test_slice_list_struct1": {{Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}}, {Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}}},
		"test_slice_list_struct2": {{Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}}, {Geo{Loc: "10.757437,106.6794102", Unit: "km", DistanceType: "plane"}}}})

	// SET

	// []int
	ctxSet := context.WithValue(ctx, goredis.CtxKey_DataType, goredis.SET)
	s, err = goredis.Get[[]int](ctxSet, "test_slice_set1")
	c.Assert(err, IsNil)
	c.Assert(s, DeepEquals, []int{1, 2})

	// multiple keys []int
	ms, err := goredis.Get[[]int](ctxSet, "test_slice_set1", "test_slice_set2")
	c.Assert(err, IsNil)
	c.Assert(ms, DeepEquals, map[string]*[]int{"test_slice_set1": {1, 2}, "test_slice_set2": {3, 4}})

	// STRING

	// []string
	s, err = goredis.Get[[]string](ctx, "test_slice_string")
	c.Assert(err, IsNil)
	c.Assert(s, DeepEquals, []string{"v1", "v2"})

	// []int
	i, err = goredis.Get[[]int](ctx, "test_slice_string_int")
	c.Assert(err, IsNil)
	c.Assert(i, DeepEquals, []int{1, 2})

	// multiple keys []int
	mi, err = goredis.Get[[]int](ctx, "test_slice_string_int", "test_slice_string_int2")
	c.Assert(err, IsNil)
	c.Assert(mi, DeepEquals, map[string]*[]int{"test_slice_string_int": {1, 2}, "test_slice_string_int2": {3, 4}})
}
