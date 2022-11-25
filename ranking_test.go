package goredis_test

import (
	"context"
	"fmt"
	"time"

	"github.com/hpi-tech/goredis"
	"github.com/hpi-tech/goutils"
	. "gopkg.in/check.v1"
)

type RankingSuite struct {
	board *goredis.RankingBoard
}

var _ = Suite(&RankingSuite{})

func (s *RankingSuite) SetUpSuite(c *C) {
	fmt.Println("SetUpSuite > RankingSuite")
	goutils.LoadEnv()
	goutils.EnableLogrus()
	goredis.Open()
	s.board = goredis.GetRankingBoard(context.Background(), "test_ranking")
}

func (s *RankingSuite) TearDownSuite(c *C) {
	goredis.Close()
}

// Test GetRankingBoard
func (s *RankingSuite) TestGetRankingBoard(c *C) {
	c.Assert(s.board, NotNil)
}

// Test UpsertMulti
func (s *RankingSuite) TestUpsertMulti(c *C) {
	scores := map[string]float64{
		"member1": 1,
		"member2": 2,
		"member3": 2,
		"member4": 4,
		"member5": 5,
	}

	err := s.board.UpsertMulti(scores, goredis.Upsert_GreaterThan)
	c.Assert(err, IsNil)
}

// Test IncrByMulti
func (s *RankingSuite) TestIncrByMulti(c *C) {
	increments := map[string]float64{
		"member1": 1,
		"member5": 5,
	}

	result, err := s.board.IncrByMulti(increments)
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
}

// Test Top
func (s *RankingSuite) TestTop(c *C) {
	result, err := s.board.Top(2, false)
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
}

// Test Scores
func (s *RankingSuite) TestScores(c *C) {
	result, err := s.board.Scores([]string{"member1", "member3"}...)
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
}

// Test Expire
func (s *RankingSuite) TestExpire(c *C) {
	err := s.board.Expire(time.Second * 30)
	c.Assert(err, IsNil)
}
