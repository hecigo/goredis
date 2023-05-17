package goredis

import (
	"context"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hecigo/goutils"
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

// Get Redis client
func (r *RankingBoard) redis() redis.UniversalClient {
	return Client(r.Context)
}

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

	return &RankingBoard{
		Id:      GetConfig(ctx).KeyPrefix + "." + strings.Join(args, "_"),
		Context: ctx,
	}
}

// Add a member to the ranking board with a score.
// Don't like [IncrBy], this method will overwrite the score of the member if it already exists.
// If a specified member is already in the sorted-set,
// the score is updated and the element reinserted at the right position to ensure the correct ordering.
// By default, only update existing elements if the new score is greater than the current score,
// unless [kind] is set to [Upsert_LessThan]. This option doesn't prevent adding new elements.
func (r *RankingBoard) Upsert(member string, score float64, kind ...RankingUpsertKind) error {
	_, err := r.redis().ZAddArgs(r.Context, r.Id, redis.ZAddArgs{
		GT:      len(kind) == 0 || (len(kind) > 0 && kind[0] == Upsert_GreaterThan),
		LT:      len(kind) > 0 && kind[0] == Upsert_LessThan,
		Members: []redis.Z{{Member: member, Score: score}},
	}).Result()

	return err
}

// Similar [Upsert], but supports multiple members. Recommended for batch operations.
func (r *RankingBoard) UpsertMulti(members map[string]float64, kind ...RankingUpsertKind) error {
	// get by pipeline
	cmds, err := r.redis().TxPipelined(r.Context, func(pipe redis.Pipeliner) error {
		for member, score := range members {
			pipe.ZAddArgs(r.Context, r.Id, redis.ZAddArgs{
				GT:      len(kind) == 0 || (len(kind) > 0 && kind[0] == Upsert_GreaterThan),
				LT:      len(kind) > 0 && kind[0] == Upsert_LessThan,
				Members: []redis.Z{{Member: member, Score: score}},
			})
		}
		return nil
	})

	if err != nil {
		return err
	}

	// parse result
	for _, cmd := range cmds {
		if cmd.Err() != nil {
			goutils.Errorf("%s", cmd.String())
			goutils.Error(cmd.Err())
		}
	}
	return nil
}

// Increment the score of a member in the ranking board.
// If the member does not exist, it is added with increment as its score.
// Returns the new score of the member.
func (r *RankingBoard) IncrBy(member string, increment float64) (float64, error) {
	rs, err := r.redis().ZIncrBy(r.Context, r.Id, increment, member).Result()
	return rs, err
}

// Similar [IncrBy], but supports multiple members.
// Returns a map of member => new score.
func (r *RankingBoard) IncrByMulti(increments map[string]float64) (map[string]float64, error) {
	// get by pipeline
	cmds, err := r.redis().TxPipelined(r.Context, func(pipe redis.Pipeliner) error {
		for member, increment := range increments {
			pipe.ZIncrBy(r.Context, r.Id, increment, member)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// parse result
	m := make(map[string]float64)
	for _, cmd := range cmds {
		c := cmd.(*redis.FloatCmd)
		err := c.Err()
		if err == nil {
			m[c.Args()[3].(string)] = c.Val()
		} else {
			goutils.Errorf("%s", c.String())
			goutils.Error(err)
		}
	}
	return m, nil
}

// Remove a member from the ranking board.
func (r *RankingBoard) Remove(member string) error {
	_, err := r.redis().ZRem(r.Context, r.Id, member).Result()
	return err
}

// Get TOP[n] members in the ranking board.
// By default, the members are ordered from highest to lowest scores, unless [orderBy] is set to [false] (~ ascending).
// Returns a map of member => score.
func (r *RankingBoard) Top(n int64, orderBy ...bool) (map[string]float64, error) {
	z, err := r.redis().ZRangeArgsWithScores(r.Context, redis.ZRangeArgs{
		Key:   r.Id,
		Start: 0,
		Stop:  n - 1,
		Rev:   len(orderBy) == 0 || (len(orderBy) > 0 && orderBy[0]),
	}).Result()

	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
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
	result, err := r.redis().ZScore(r.Context, r.Id, member).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return result, err
}

// Get scores of multiple members in the ranking board.
// Returns a map of member => score.
func (r *RankingBoard) Scores(members ...string) (map[string]float64, error) {
	// get by pipeline
	cmds, err := r.redis().TxPipelined(r.Context, func(pipe redis.Pipeliner) error {
		for _, member := range members {
			pipe.ZScore(r.Context, r.Id, member)
		}
		return nil
	})

	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	// parse result
	m := make(map[string]float64)
	for i, cmd := range cmds {
		err = cmd.Err()
		if err == nil {
			m[members[i]] = cmd.(*redis.FloatCmd).Val()
		} else {
			if err != redis.Nil {
				goutils.Errorf("%s", cmd.String())
				goutils.Error(err)
			}
			m[members[i]] = 0
		}
	}
	return m, nil
}

// Delete the ranking board.
func (r *RankingBoard) Delete() error {
	_, err := r.redis().Del(r.Context, r.Id).Result()
	return err
}

// Set expiration time of the ranking board.
func (r *RankingBoard) Expire(ttl time.Duration) error {
	_, err := r.redis().Expire(r.Context, r.Id, ttl).Result()
	return err
}
