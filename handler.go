package goredis

import (
	"context"
	"time"

	"errors"
	"reflect"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/hpi-tech/goutils"
)

type ctxKeyType_Redis string

const (
	HASH                                 = "hash"
	LIST                                 = "list"
	SET                                  = "set"
	ZSET                                 = "zset" // sorted-set
	STRING                               = "string"
	Stream                               = "stream"
	CtxKey_DataType     ctxKeyType_Redis = "redis_data_type"
	CtxKey_SliceStart   ctxKeyType_Redis = "redis_slice_start"
	CtxKey_SliceStop    ctxKeyType_Redis = "redis_slice_stop"
	CtxKey_SliceReverse ctxKeyType_Redis = "redis_slice_rev"
)

// Get one or multiple values by key(s). If the key does not exist, the value will be nil.
// In case of multiple keys, the result will be a map of key-value pairs. All values must be of the same type.
//
// # Parameters:
//
//  1. [T]: the type of the value
//
//     - As default, it will get value from Redis [STRING] key.
//
//     - T is a map[string]interface{} and [ctx] has [CtxKey_DataType] = [HASH], it will get value from Redis [HASH] key.
//     Because of Redis limitation, if T is a map of struct, recommended to use only non-nested struct.
//
//     - T is a slice, and [ctx] has [CtxKey_DataType] = [LIST], it will get value from Redis [LIST] key;
//     [ctx] has [CtxKey_DataType] = [SET], it will get value from Redis [SET] key;
//     [ctx] has [CtxKey_DataType] = [ZSET], it will get value from Redis [ZSET] key (sorted-set).
//
//  2. [ctx]:
//
//     - Including [goutils.CtxKey_ConnName] to specify the connection name. It also used to get the key prefix and track APM.
//
//     - Including [CtxKey_DataType] to specify the data type of the Redis key.
//
//  3. [keys]: the key without prefix.
//
// # Notes:
//
//  1. Should not use this function to get a long list/set. It will cause performance issue.
//
//  2. Allowing get a range from Redis [LIST] or [ZSET] (sorted-set) by setting [ctx] with values:
//
//     - [CtxKey_SliceStart]: the start index of the range
//
//     - [CtxKey_SliceStop]: the stop index of the range
//
//     - [CtxKey_SliceReverse]: true (default) to order the sorted-set by descending of score
func Get[T any](ctx context.Context, keys ...string) (interface{}, error) {
	if len(keys) == 0 {
		return nil, errors.New("keys is empty")
	}

	// get type of T
	var t T
	tKind := reflect.TypeOf(t).Kind()

	switch tKind {
	// string
	case reflect.String:
		return getString(ctx, keys...)

	// map[string]any or struct
	case reflect.Map, reflect.Struct:
		switch ctx.Value(CtxKey_DataType) {
		case HASH:
			return getHash[T](ctx, keys...)
		default:
			return getVariousKind[T](ctx, keys...)
		}

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

	// time.Time, time.Duration, struct and various kinds of int, float, bool...
	default:
		return getVariousKind[T](ctx, keys...)
	}
}

// Set value(s) to a unique key. If the key already exists, it will be overwritten.
//
// # Parameters:
//
//  1. [value]:
//
//     - As default, it will set value to Redis [STRING] key.
//
//     - If value is a map[string]interface{} or struct and [ctx] has [CtxKey_DataType] = [HASH], it will set to Redis [HASH].
//     Because of Redis limitation, if value is a map of struct, recommended to use non-nested struct.
//
//     - If value is a slice, and [ctx] has [CtxKey_DataType] = [LIST], it will set to Redis [LIST];
//     [ctx] has [CtxKey_DataType] = [SET], it will set to Redis [SET];
//
//  2. expiration: time-to-live of the key. If it is 0 or omitted, the key will never expire.
//
// # Notes:
//
//  1. This function does not support to set value to Redis [ZSET] because [ZSET] is a special data type.
//
//  2. This function will delete the old key first, then set the new list/set, sothat should not use it to set a long list/set.
func Set(ctx context.Context, key string, value interface{}, expiration ...time.Duration) error {
	if key == "" {
		return errors.New("key is empty")
	}
	if value == nil {
		return errors.New("value is nil")
	}

	var expi time.Duration = 0 // never expire
	if len(expiration) > 1 {
		expi = expiration[0]
	}

	// get type of value
	tKind := reflect.TypeOf(value).Kind()

	switch tKind {
	// string
	case reflect.String:
		return setVariousKind(ctx, key, value, expi)

	// map[string]any or struct
	case reflect.Map, reflect.Struct:
		switch ctx.Value(CtxKey_DataType) {
		case HASH:
			return setHash(ctx, key, value, expi)
		default:
			return setVariousKind(ctx, key, value, expi)
		}

	// slice
	case reflect.Slice:
		switch ctx.Value(CtxKey_DataType) {
		case SET:
			return setSet(ctx, key, value, expi)
		case LIST:
			return setList(ctx, key, value, expi)
		default:
			return setVariousKind(ctx, key, value, expi)
		}

	// time.Time, time.Duration, struct and various kinds of int, float, bool...
	default:
		return setVariousKind(ctx, key, value, expi)
	}
}

// Similar to [Set], but it support to set multiple keys at once.
// Example: Set multiple Redis [HASH] at once.
//
//	var keyValues = make(map[string]interface{})
//	// hash 1
//	keyValues["hash1"] = map[string]interface{}{"field1": "value11", "field2": "value21"}
//	// hash 2
//	keyValues["hash2"] = map[string]interface{}{"field1": "value12", "field2": "value22"}
//
//	ctx := context.WithValue(context.Background(), goredis.CtxKey_DataType, goredis.HASH)
//	MSet(ctx, keyValues)
func MSet(ctx context.Context, keyValues map[string]interface{}, expiration ...time.Duration) error {
	var expi time.Duration = 0 // never expire
	if len(expiration) > 1 {
		expi = expiration[0]
	}

	switch elKind := reflect.TypeOf(keyValues).Elem().Kind(); elKind {
	// string
	case reflect.String:
		return setMultiVariousKind(ctx, keyValues, expi)

	// map[string]any or struct
	case reflect.Map, reflect.Struct:
		switch ctx.Value(CtxKey_DataType) {
		case HASH:
			return setMultiHash(ctx, keyValues, expi)
		default:
			return setMultiVariousKind(ctx, keyValues, expi)
		}

	// slice
	case reflect.Slice:
		switch ctx.Value(CtxKey_DataType) {
		case SET:
			return setMultiSet(ctx, keyValues, expi)
		case LIST:
			return setMultiList(ctx, keyValues, expi)
		default:
			return setMultiVariousKind(ctx, keyValues, expi)
		}

	default:
		return setMultiVariousKind(ctx, keyValues, expi)
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

// Set any value to Redis as string.
func setVariousKind(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	status, err := Client(ctx).Set(ctx, addKeyPrefix(ctx, key)[0], value, expiration).Result()
	if err != nil {
		return err
	}
	if status != "OK" {
		goutils.Errorf("setVariousKind: status is %s", status)
		goutils.Errorf("key is %s", key)
		goutils.Errorf(" value is %v", value)
		return errors.New("redis set status is not OK")
	}
	return nil
}

// Set multiple key-values to Redis as string.
func setMultiVariousKind(ctx context.Context, keyValues map[string]interface{}, expiration time.Duration) error {
	// add key prefix and convert keyValues to slice
	var kv []interface{}
	for k, v := range keyValues {
		kv = append(kv, addKeyPrefix(ctx, k)[0], v)
	}

	// set
	status, err := Client(ctx).MSet(ctx, kv).Result()
	if err != nil {
		return err
	}
	if status != "OK" {
		goutils.Errorf("mSetVariousKind: status is %s", status)
		goutils.Errorf("keyValues is %v", keyValues)
		return errors.New("redis set status is not OK")
	}
	return nil
}

// Get hash from Redis.
func getHash[T any](ctx context.Context, keys ...string) (interface{}, error) {
	if len(keys) == 0 {
		return nil, errors.New("key is empty")
	}

	// T is struct
	var t T
	tIsTruct := reflect.TypeOf(t).Kind() == reflect.Struct

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

// Set hash to Redis. The value must be a struct or a map.
func setHash(ctx context.Context, key string, value interface{}, expiration time.Duration) (err error) {

	// convert value to map[string]interface{}
	var temp map[string]interface{}
	switch value := value.(type) {
	case map[string]interface{}:
		temp = value
	default:
		temp, err = goutils.Unmarshal[map[string]interface{}](value)
		if err != nil {
			return err
		}
	}

	// convert temp to slice of interface{}
	var val []interface{}
	for k, v := range temp {
		val = append(val, k, v)
	}

	// set key-value
	key = addKeyPrefix(ctx, key)[0]
	status, err := Client(ctx).HMSet(ctx, key, val...).Result()
	if err != nil {
		return err
	}
	if !status {
		goutils.Errorf("setHash: status is %v", status)
		goutils.Errorf("key is %s", key)
		goutils.Errorf(" value is %v", value)
		return errors.New("redis set status is not OK")
	}

	// set expiration
	err = setExpiration(ctx, key, expiration)
	return err
}

// Similar to [setHash], but support multiple key-values with pipeline.
func setMultiHash(ctx context.Context, keyValues map[string]interface{}, expiration time.Duration) (err error) {
	// convert keyValues to map[string][]interface{}
	var temp map[string][]interface{}
	for key, value := range keyValues {
		var val []interface{}
		switch v := value.(type) {
		case map[string]interface{}:
			for k, v := range v {
				val = append(val, k, v)
			}
		default:
			m, err := goutils.Unmarshal[map[string]interface{}](v)
			if err != nil {
				return err
			}
			for k, v := range m {
				val = append(val, k, v)
			}
		}
		temp[key] = val
	}

	// set key-values
	cmds, err := Client(ctx).TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for key, val := range temp {
			pipe.HMSet(ctx, addKeyPrefix(ctx, key)[0], val...)
			pipe.Expire(ctx, addKeyPrefix(ctx, key)[0], expiration)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// check status
	for _, cmd := range cmds {
		err := cmd.Err()
		if err != nil {
			key := removeKeyPrefix(ctx, cmd.Args()[1].(string))[0]
			goutils.Errorf("setMultiHash: %v", err)
			goutils.Errorf("key is %s", key)
			goutils.Errorf("value is %v", keyValues[key])
			return err
		}
	}

	return nil
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

// Set list to Redis. The value must be a slice.
// This action will delete the old list and set a new one.
func setList(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	val := value.([]interface{})
	key = addKeyPrefix(ctx, key)[0]

	// delete old list
	_, err := Client(ctx).Del(ctx, key).Result()
	if err != nil {
		return err
	}

	// set new list
	_, err = Client(ctx).RPush(ctx, key, val...).Result()
	if err != nil {
		return err
	}

	// set expiration
	err = setExpiration(ctx, key, expiration)
	return err
}

// Similar to [setList], but support multiple key-values with pipeline.
func setMultiList(ctx context.Context, keyValues map[string]interface{}, expiration time.Duration) (err error) {
	// convert keyValues to map[string][]interface{}
	var temp map[string][]interface{}
	for key, value := range keyValues {
		temp[key] = value.([]interface{})
	}

	// set key-values
	cmds, err := Client(ctx).TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for key, val := range temp {
			key := addKeyPrefix(ctx, key)[0]
			pipe.Del(ctx, key)
			pipe.RPush(ctx, key, val...)
			pipe.Expire(ctx, key, expiration)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// check status
	for _, cmd := range cmds {
		err := cmd.Err()

		// get key from cmd
		key := removeKeyPrefix(ctx, cmd.Args()[1].(string))[0]

		if err != nil {
			goutils.Errorf("setMultiList: %v", err)
			goutils.Errorf("key is %s", key)
			goutils.Errorf("value is %v", keyValues[key])
			return err
		}
	}
	return nil
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

// Set set to Redis. The value must be a slice.
// This action will delete the old set and set a new one.
func setSet(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	val := value.([]interface{})
	key = addKeyPrefix(ctx, key)[0]

	// delete old set
	_, err := Client(ctx).Del(ctx, key).Result()
	if err != nil {
		return err
	}

	// set new set
	_, err = Client(ctx).SAdd(ctx, key, val...).Result()
	if err != nil {
		return err
	}

	// set expiration
	err = setExpiration(ctx, key, expiration)
	return err
}

// Similar to [setSet], but support multiple key-values with pipeline.
func setMultiSet(ctx context.Context, keyValues map[string]interface{}, expiration time.Duration) (err error) {
	// convert keyValues to map[string][]interface{}
	var temp map[string][]interface{}
	for key, value := range keyValues {
		temp[key] = value.([]interface{})
	}

	// set key-values
	cmds, err := Client(ctx).TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for key, val := range temp {
			key := addKeyPrefix(ctx, key)[0]
			pipe.Del(ctx, key)
			pipe.SAdd(ctx, key, val...)
			pipe.Expire(ctx, key, expiration)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// check status
	for _, cmd := range cmds {
		err := cmd.Err()

		// get key from cmd
		key := removeKeyPrefix(ctx, cmd.Args()[1].(string))[0]

		if err != nil {
			goutils.Errorf("setMultiSet: %v", err)
			goutils.Errorf("key is %s", key)
			goutils.Errorf("value is %v", keyValues[key])
			return err
		}
	}
	return nil
}

// Get sorted-set from Redis.
func getSortedSet[T any](ctx context.Context, keys ...string) (interface{}, error) {
	if len(keys) == 0 {
		return nil, errors.New("key is empty")
	}

	var (
		t     T
		start int64 = 0
		stop  int64 = -1
		rev         = true
	)

	// get sorted-set start and stop from context
	i := ctx.Value(CtxKey_SliceStart)
	if i != nil {
		start = i.(int64)
	}
	i = ctx.Value(CtxKey_SliceStop)
	if i != nil {
		stop = i.(int64)
	}
	i = ctx.Value(CtxKey_SliceReverse)
	if i != nil {
		rev = i.(bool)
	}

	// get single key-value with ZRangeArgs
	if len(keys) == 1 {
		cmd := Client(ctx).ZRangeArgs(ctx, redis.ZRangeArgs{
			Key:   addKeyPrefix(ctx, keys...)[0],
			Start: start,
			Stop:  stop,
			Rev:   rev,
		})
		return redisCmdToSlice[T](ctx, reflect.TypeOf(t).Elem(), cmd)
	}

	// get multiple key-values
	cmds, err := Client(ctx).TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, k := range addKeyPrefix(ctx, keys...) {
			pipe.ZRangeArgs(ctx, redis.ZRangeArgs{
				Key:   k,
				Start: start,
				Stop:  stop,
				Rev:   rev,
			})
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

// set expiration for key
func setExpiration(ctx context.Context, key string, expiration time.Duration) error {
	if expiration == 0 {
		return nil
	}

	status, err := Client(ctx).Expire(ctx, key, expiration).Result()
	if err != nil {
		return err
	}
	if !status {
		goutils.Errorf("setExpiration: status is %v", status)
		goutils.Errorf("key is %s", key)
		return errors.New("redis set status is not OK")
	}

	return nil
}
