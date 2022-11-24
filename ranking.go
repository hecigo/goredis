package goredis

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/hpi-tech/goutils"
)

type RankingBoard struct {
	Id      string          `json:"id"`
	Context context.Context `json:"-"`
}

type RankingUpsertKind int

const (
	Upsert_GreaterThan RankingUpsertKind = 0
	Upsert_LessThan    RankingUpsertKind = 1
)

// Initialize a ranking board, and return a RankingBoard instance.
// The ranking board is a sorted set in redis.
// It will be created if does not exist automatically when [Upsert] or [IncrBy] called at the first time.
// [args]:
//   - At least one argument is required, the first argument is [RankingBoard].Id.
//     It is also the key of the sorted-set in redis.
//   - The second argument is optional, it is the name of the redis connection.
func GetRankingBoard(ctx context.Context, args ...string) *RankingBoard {
	if len(args) == 0 {
		return nil
	}

	if len(args) > 1 {
		ctx = context.WithValue(ctx, goutils.CtxKey_ConnName, args[1])
	}

	return &RankingBoard{
		Id:      args[0],
		Context: ctx,
	}
}

// Add a member to the ranking board with a score.
// Don't like [IncrBy], this method will overwrite the score of the member if it already exists.
// If a specified member is already in the sorted-set,
// the score is updated and the element reinserted at the right position to ensure the correct ordering.
// As default, only update existing elements if the new score is greater than the current score,
// unless [kind] is set to [Upsert_LessThan]. This option doesn't prevent adding new elements.
func (r *RankingBoard) Upsert(member string, score float64, kind ...RankingUpsertKind) error {
	_, err := r.redis().ZAddArgs(r.Context, r.Id, redis.ZAddArgs{
		GT:      len(kind) == 0 || (len(kind) > 0 && kind[0] == Upsert_GreaterThan),
		LT:      len(kind) > 0 && kind[0] == Upsert_LessThan,
		Members: []redis.Z{{Member: member, Score: score}},
	}).Result()

	return err
}

// Remove a member from the ranking board.
func (r *RankingBoard) Remove(member string) error {
	_, err := r.redis().ZRem(r.Context, r.Id, member).Result()
	return err
}

// Increment the score of a member in the ranking board.
// If the member does not exist, it is added with increment as its score.
// Returns the new score of the member.
func (r *RankingBoard) IncrBy(member string, increment float64) (float64, error) {
	return r.redis().ZIncrBy(r.Context, r.Id, increment, member).Result()
}

// Get TOP[n] members in the ranking board.
// As default, the members are ordered from highest to lowest scores, unless [orderBy] is set to [false] (~ ascending).
// Returns a map of member => score.
func (r *RankingBoard) Top(n int64, orderBy ...bool) (map[string]float64, error) {
	z, err := r.redis().ZRangeArgsWithScores(r.Context, redis.ZRangeArgs{
		Key:     r.Id,
		Start:   0,
		Stop:    -1,
		ByScore: true,
		Offset:  0,
		Count:   n,
		Rev:     len(orderBy) == 0 || (len(orderBy) > 0 && orderBy[0]),
	}).Result()

	if err != nil {
		return nil, err
	}

	m := make(map[string]float64)
	for _, v := range z {
		m[v.Member.(string)] = v.Score
	}
	return m, nil
}

// Get score of a member in the ranking board.
func (r *RankingBoard) Score(member string) (float64, error) {
	return r.redis().ZScore(r.Context, r.Id, member).Result()
}

// Get Redis client
func (r *RankingBoard) redis() redis.UniversalClient {
	return Client(r.Context)
}
