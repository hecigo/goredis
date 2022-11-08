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

// Test for Get[T any](ctx context.Context, keys ...string) (*T, error)
// Parameters:
// - keys: []string {"test"}
func (s *MySuite) TestGet1Key(c *C) {
	// get value from redis
	ctx := context.Background()
	_, err := goredis.Get[string](ctx, "test")
	c.Assert(err, IsNil)
}

// Test for Get[T any](ctx context.Context, keys ...string) (*T, error)
// Parameters:
// - keys: []string {"test1","test2"}
func (s *MySuite) TestGet2Keys(c *C) {
	// get value from redis
	ctx := context.Background()
	_, err := goredis.Get[map[string]*string](ctx, "test1", "test2")
	c.Assert(err, IsNil)
}
