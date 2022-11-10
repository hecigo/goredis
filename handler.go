package goredis

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/hpi-tech/goutils"
)

type ctxKeyType_RedisDataType string

const (
	HASH                                          = "hash"
	LIST                                          = "list"
	SET                                           = "set"
	ZSET                                          = "zset"
	STRING                                        = "string"
	Stream                                        = "stream"
	CtxKey_RedisDataType ctxKeyType_RedisDataType = "redis_data_type"
)

// Get one or multiple values by key(s). If the key does not exist, the value will be nil.
// In case of multiple keys, the result will be a map of key-value pairs. All values must be of the same type.
//
// # Parameters:
//
//  1. [T]: the type of the value
//
//     - T is string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool, time.Time, time.Duration or struct,
//     then it will get from Redis STRING key and convert to the given type.
//
//     - T is a map[string]interface{}, then it always get from Redis HASH and return to a map[string]string (because Redis does not support other types).
//
//     - T is a slice, then it get from Redis [STRING] as default;
//     from Redis LIST if [ctx] contains key [CtxKey_RedisDataType] with value [LIST]; or
//     from Redis SET if [ctx] contains key [CtxKey_RedisDataType] with value [SET].
//
//  2. [ctx]: includes [goutils.CtxConnNameKey] to specify the connection name. It also used to get the key prefix and track APM.
//
//  3. [keys]: the key without prefix.
//
// # Notes:
//
//  1. This function would return all elements in a Redis [LIST] or [SET], so it is not recommended to use it for a long list/set.
func Get[T any](ctx context.Context, keys ...string) (interface{}, error) {
	if len(keys) == 0 {
		return nil, errors.New("keys is empty")
	}

	// get type of T
	var t T
	tType := reflect.TypeOf(t)
	tKind := tType.Kind()

	switch tKind {
	// string
	case reflect.String:
		return getString(ctx, keys...)

	// map[string]interface{}
	case reflect.Map:
		return getHash[T](ctx, keys...)

	// slice
	case reflect.Slice:
		switch ctx.Value(CtxKey_RedisDataType) {
		case SET:
			return getSet[T](ctx, keys...)
		case LIST:
			return getList[T](ctx, keys...)
		default:
			return getVariousKind[T](ctx, keys...)
		}

	// time.Time, time.Duration, struct and various kinds of int, float, bool...
	default:
		return getVariousKind[T](ctx, keys...)
	}
}

// add key prefix to the given key
func addKeyPrefix(ctx context.Context, key ...string) []string {
	if len(key) == 0 {
		goutils.Panic("key is empty")
	}

	cfg := GetConfig(ctx)
	for i, k := range key {
		key[i] = cfg.KeyPrefix + "." + k
	}
	return key
}

// remove key prefix from the given key
func removeKeyPrefix(ctx context.Context, key ...string) []string {
	if len(key) == 0 {
		goutils.Panic("key is empty")
	}

	cfg := GetConfig(ctx)
	for i, k := range key {
		key[i] = strings.TrimPrefix(k, cfg.KeyPrefix+".")
	}
	return key
}

// Get string(s) from Redis.
func getString(ctx context.Context, keys ...string) (interface{}, error) {
	if len(keys) == 0 {
		return nil, errors.New("key is empty")
	}

	// get single key-value
	if len(keys) == 1 {
		val, err := Client(ctx).Get(ctx, addKeyPrefix(ctx, keys...)[0]).Result()
		if err != nil {
			if err == redis.Nil {
				// redis.Nil means the key does not exist, so we just return nil
				return nil, nil
			}
			return nil, err
		}
		return val, nil
	}

	// get multiple key-values
	val, err := Client(ctx).MGet(ctx, addKeyPrefix(ctx, keys...)...).Result()

	// error
	if err != nil {
		if err == redis.Nil {
			// redis.Nil means the key does not exist, so we just return nil
			return nil, nil
		}
		return nil, err
	}

	// result
	r := make(map[string]*string)
	for i, k := range removeKeyPrefix(ctx, keys...) {
		v := val[i]
		if v == nil {
			r[k] = nil
		} else {
			val := val[i].(string)
			r[k] = &val
		}
	}
	return r, nil
}

// Get string(s) from Redis and convert to the given type.
func getVariousKind[T any](ctx context.Context, keys ...string) (interface{}, error) {
	val, err := getString(ctx, keys...)
	if err != nil {
		return nil, err
	}

	switch val := val.(type) {
	case string:
		return goutils.StrConv[T](val)
	case map[string]*string:
		return goutils.MapPtrStrConv[T](val)
	default:
		return nil, errors.New("result type is not string or map[string]string")
	}
}

// Get hash from Redis.
func getHash[T any](ctx context.Context, keys ...string) (interface{}, error) {
	if len(keys) == 0 {
		return nil, errors.New("key is empty")
	}

	// get single key-value
	if len(keys) == 1 {
		val, err := Client(ctx).HGetAll(ctx, addKeyPrefix(ctx, keys...)[0]).Result()
		if err != nil {
			if err == redis.Nil {
				// redis.Nil means the key does not exist, so we just return nil
				return nil, nil
			}
			return nil, err
		}

		return val, nil
	}

	// get multiple key-values
	cmds, err := Client(ctx).TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, k := range addKeyPrefix(ctx, keys...) {
			pipe.HGetAll(ctx, k)
		}
		return nil
	})
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	r := make(map[string]*map[string]string)
	for i, k := range removeKeyPrefix(ctx, keys...) {
		val := cmds[i].(*redis.StringStringMapCmd).Val()
		if len(val) == 0 {
			r[k] = nil
		} else {
			r[k] = &val
		}
	}

	return r, nil
}

// Get set from Redis.
func getSet[T any](ctx context.Context, keys ...string) (interface{}, error) {
	if len(keys) == 0 {
		return nil, errors.New("key is empty")
	}

	var t T

	// get single key-value
	if len(keys) == 1 {
		cmd := Client(ctx).SMembers(ctx, addKeyPrefix(ctx, keys...)[0])
		return redisCmdToSlice[T](ctx, reflect.TypeOf(t).Elem(), cmd)
	}

	// get multiple key-values
	cmds, err := Client(ctx).TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, k := range addKeyPrefix(ctx, keys...) {
			pipe.SMembers(ctx, k)
		}
		return nil
	})

	return redisCmdToMap[T](ctx, reflect.TypeOf(t).Elem(), keys, cmds, err)
}

// Get all elements of list from Redis.
func getList[T any](ctx context.Context, keys ...string) (interface{}, error) {
	if len(keys) == 0 {
		return nil, errors.New("key is empty")
	}

	var t T

	// get single key-value
	if len(keys) == 1 {
		cmd := Client(ctx).LRange(ctx, addKeyPrefix(ctx, keys...)[0], 0, -1)
		return redisCmdToSlice[T](ctx, reflect.TypeOf(t).Elem(), cmd)
	}

	// get multiple key-values
	cmds, err := Client(ctx).TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, k := range addKeyPrefix(ctx, keys...) {
			pipe.LRange(ctx, k, 0, -1)
		}
		return nil
	})

	return redisCmdToMap[T](ctx, reflect.TypeOf(t).Elem(), keys, cmds, err)
}

func redisCmdToSlice[T any](ctx context.Context, eleType reflect.Type, cmd *redis.StringSliceCmd) (interface{}, error) {
	if cmd == nil {
		return nil, errors.New("cmd is nil")
	}

	val, err := cmd.Result()
	if err != nil {
		if err == redis.Nil {
			// redis.Nil means the key does not exist, so we just return nil
			return nil, nil
		}
		return nil, err
	}

	temp, err := goutils.ReflectSliceStrConv(val, eleType)
	if err != nil {
		return nil, err
	}
	return goutils.Unmarshal[T](temp)
}

func redisCmdToMap[T any](ctx context.Context, eleType reflect.Type, keys []string, cmds []redis.Cmder, connErr error) (interface{}, error) {
	if connErr != nil {
		if connErr == redis.Nil {
			return nil, nil
		}
		return nil, connErr
	}

	r := make(map[string]interface{})
	for i, k := range removeKeyPrefix(ctx, keys...) {
		c := cmds[i].(*redis.StringSliceCmd)
		if c.Err() == redis.Nil {
			r[k] = nil
		} else {
			val := c.Val()
			if len(val) == 0 {
				r[k] = nil
			} else {
				temp, err := goutils.ReflectSliceStrConv(val, eleType)
				if err != nil {
					r[k] = err
				}

				r[k], err = goutils.Unmarshal[T](temp)
				if err != nil {
					r[k] = err
				}
			}
		}
	}

	return goutils.Unmarshal[map[string]*T](r)
}
