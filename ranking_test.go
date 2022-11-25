package goredis_test

import (
	"context"
	"testing"

	"github.com/hpi-tech/goredis"
	"github.com/hpi-tech/goutils"
	. "gopkg.in/check.v1"
)

func TestRanking(t *testing.T) { TestingT(t) }

type RankingSuite struct {
	board *goredis.RankingBoard
}

var _ = Suite(&HandlerSuite{})

func (s *RankingSuite) SetUpSuite(c *C) {
	if *testCase != "ranking" {
		c.Skip("bypass `ranking` test")
		return
	}

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
