package goredis

import (
	"context"

	"errors"
	"reflect"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/hpi-tech/goutils"
)

type ctxKeyType_Redis string

const (
	HASH                               = "hash"
	LIST                               = "list"
	SET                                = "set"
	ZSET                               = "zset" // sorted set
	STRING                             = "string"
	Stream                             = "stream"
	CtxKey_DataType   ctxKeyType_Redis = "redis_data_type"
	CtxKey_SliceStart ctxKeyType_Redis = "redis_slice_start"
	CtxKey_SliceStop  ctxKeyType_Redis = "redis_slice_stop"
)

// Get one or multiple values by key(s). If the key does not exist, the value will be nil.
// In case of multiple keys, the result will be a map of key-value pairs. All values must be of the same type.
//
// # Parameters:
//
//  1. [T]: the type of the value
//
//     - T is string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool, time.Time, time.Duration or struct,
//     it will get from Redis [STRING] key and convert to the given type.
//
//     - T is a map[string]any, it always get from Redis [HASH]. Because of Redis limitation, if T is a map of struct, recommended to use non-nested struct.
//
//     - T is a slice, it will get from Redis [STRING] as default;
//     from Redis [LIST] if [ctx] contains key [CtxKey_RedisDataType] with value [LIST]; or
//     from Redis [SET] if [ctx] contains key [CtxKey_RedisDataType] with value [SET].
//
//  2. [ctx]: includes [goutils.CtxConnNameKey] to specify the connection name. It also used to get the key prefix and track APM.
//
//  3. [keys]: the key without prefix.
//
// # Notes:
//
//  1. This function would return all elements in a Redis [LIST] or [SET] as default, so it is not recommended to use it for a long list/set.
//  2. Allowing get a range from Redis [LIST] or [ZSET] (sorted set) by setting [ctx] with key [CtxKey_SliceStart] and [CtxKey_SliceStop].
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
		switch ctx.Value(CtxKey_DataType) {
		case SET:
			return getSet[T](ctx, keys...)
		case ZSET:
			return getSortedSet[T](ctx, keys...)
		case LIST:
			return getList[T](ctx, keys...)
		default:
			return getVariousKind[T](ctx, keys...)
		}

	case reflect.Struct:
		switch ctx.Value(CtxKey_DataType) {
		case HASH:
			return getHash[T](ctx, keys...)
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

	tIsTruct := ctx.Value(CtxKey_DataType) == HASH

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

		if tIsTruct {
			return goutils.Unmarshal[T](val)
		}
		return goutils.MapStrConv[T](val)
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

	r := make(map[string]interface{})
	for i, k := range removeKeyPrefix(ctx, keys...) {
		c := cmds[i].(*redis.StringStringMapCmd)
		err := c.Err()
		if err != nil {
			r[k] = nil
			if err != redis.Nil {
				goutils.Error(err)
			}
		} else {
			val := c.Val()
			if len(val) == 0 {
				r[k] = nil
			} else {
				if tIsTruct {
					r[k], err = goutils.Unmarshal[T](val)
				} else {
					r[k], err = goutils.MapStrConv[T](val)
				}
				if err != nil {
					goutils.Error(err)
				}
			}
		}
	}

	return goutils.Unmarshal[map[string]*T](r)
}

// Get all elements of list from Redis.
func getList[T any](ctx context.Context, keys ...string) (interface{}, error) {
	if len(keys) == 0 {
		return nil, errors.New("key is empty")
	}

	var (
		t     T
		start int64 = 0
		stop  int64 = -1
	)

	// get list start and stop from context
	i := ctx.Value(CtxKey_SliceStart)
	if i != nil {
		start = i.(int64)
	}
	i = ctx.Value(CtxKey_SliceStop)
	if i != nil {
		stop = i.(int64)
	}

	// get single key-value
	if len(keys) == 1 {
		cmd := Client(ctx).LRange(ctx, addKeyPrefix(ctx, keys...)[0], start, stop)
		return redisCmdToSlice[T](ctx, reflect.TypeOf(t).Elem(), cmd)
	}

	// get multiple key-values
	cmds, err := Client(ctx).TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, k := range addKeyPrefix(ctx, keys...) {
			pipe.LRange(ctx, k, start, stop)
		}
		return nil
	})

	return redisCmdToMap[T](ctx, reflect.TypeOf(t).Elem(), keys, cmds, err)
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

// Get sorted set from Redis.
func getSortedSet[T any](ctx context.Context, keys ...string) (interface{}, error) {
	if len(keys) == 0 {
		return nil, errors.New("key is empty")
	}

	var (
		t     T
		start int64 = 0
		stop  int64 = -1
	)

	// get sorted set start and stop from context
	i := ctx.Value(CtxKey_SliceStart)
	if i != nil {
		start = i.(int64)
	}
	i = ctx.Value(CtxKey_SliceStop)
	if i != nil {
		stop = i.(int64)
	}

	// get single key-value
	if len(keys) == 1 {
		cmd := Client(ctx).ZRange(ctx, addKeyPrefix(ctx, keys...)[0], start, stop)
		return redisCmdToSlice[T](ctx, reflect.TypeOf(t).Elem(), cmd)
	}

	// get multiple key-values
	cmds, err := Client(ctx).TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, k := range addKeyPrefix(ctx, keys...) {
			pipe.ZRange(ctx, k, start, stop)
		}
		return nil
	})

	return redisCmdToMap[T](ctx, reflect.TypeOf(t).Elem(), keys, cmds, err)
}

// read redis command result to slice
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

// read redis command result to map
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
		err := c.Err()
		if err != nil {
			r[k] = nil
			if err != redis.Nil {
				goutils.Error(err)
			}
		} else {
			val := c.Val()
			if len(val) == 0 {
				r[k] = nil
			} else {
				temp, err := goutils.ReflectSliceStrConv(val, eleType)
				if err != nil {
					r[k] = nil
					goutils.Error(err)
				}

				r[k], err = goutils.Unmarshal[T](temp)
				if err != nil {
					r[k] = nil
					goutils.Error(err)
				}
			}
		}
	}

	return goutils.Unmarshal[map[string]*T](r)
}
