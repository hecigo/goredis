package goredis_test

import (
	"context"
	"fmt"

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
