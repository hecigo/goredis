package goredis

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/go-redis/redis/v8"
	json "github.com/goccy/go-json"
)

// Get value from redis. If the value is not found, return nil.
// It supports to get multiple values at once.
//
// Parameters:
//   - [T]: the type of the value, e.g. string, int, struct, etc. In case of multiple values, it must be a map of string to pointer of the type. e.g. map[string]*string
//   - [ctx]: includes [goutils.CtxConnNameKey] to specify the connection name. It also used to get the key prefix and track APM.
//   - [key]: is the key to get without prefix.
func Get[T any](ctx context.Context, keys ...string) (*T, error) {

	if len(keys) == 0 {
		return nil, errors.New("keys is empty")
	}

	if len(keys) == 1 {
		// just get one key-value
		val, err := Client(ctx).Get(ctx, addKeyPrefix(ctx, keys[0])).Result()
		if err != nil {
			if err == redis.Nil {
				return nil, nil
			}
			return nil, err
		}
		return unmarshal[T](val)
	} else {
		// get multiple keys-values
		var t T
		tType := reflect.TypeOf(t)

		// T must be a map of string to pointer
		if tType.Kind() != reflect.Map ||
			tType.Key().Kind() != reflect.String ||
			tType.Elem().Kind() != reflect.Ptr {
			return nil, errors.New("generic type `T` must be a map of string to pointer. e.g. map[string]*User")
		}

		// create a pipeline
		cmds, err := Client(ctx).TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, k := range keys {
				pipe.Get(ctx, addKeyPrefix(ctx, k))
			}
			return nil
		})

		if err != nil {
			if err == redis.Nil {
				// redis.Nil means the key does not exist, so we just return nil
				return nil, nil
			}
			return nil, err
		}

		// create the result map of string to pointer
		result := reflect.MakeMap(tType)
		for _, cmd := range cmds {
			e := cmd.Err()
			strCmd := cmd.(*redis.StringCmd)
			k := removeKeyPrefix(ctx, strCmd.Args()[1].(string))

			if e != nil {
				if e == redis.Nil {
					// redis.Nil means the key does not exist, so we just skip it
					result.SetMapIndex(reflect.ValueOf(k), reflect.Zero(tType.Elem()))
					continue
				}
				return nil, e
			}

			// get value from Redis CMD
			v := strCmd.Val()

			// unmarshal value to the given kind or struct
			eleType := tType.Elem().Elem() // tType.Elem() is *P, tType.Elem().Elem() is P
			val, err := reflectUnmarshal(eleType, v)
			if err != nil {
				return nil, err
			}

			// set value to the result map
			valP := reflect.New(eleType)
			valP.Elem().Set(reflect.ValueOf(val))
			result.SetMapIndex(reflect.ValueOf(k), valP)
		}

		// convert result to *T
		resultP := reflect.New(tType)
		resultP.Elem().Set(result)
		return resultP.Interface().(*T), nil
	}
}

// add key prefix to the given key
func addKeyPrefix(ctx context.Context, key string) string {
	cfg := GetConfig(ctx)
	return cfg.KeyPrefix + "." + key
}

// remove key prefix from the given key
func removeKeyPrefix(ctx context.Context, key string) string {
	cfg := GetConfig(ctx)
	return key[len(cfg.KeyPrefix)+1:]
}

// unmarshal value to the given kind or struct using reflection.
func reflectUnmarshal(t reflect.Type, v string) (interface{}, error) {
	switch t.Kind() {
	case reflect.String,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Float32,
		reflect.Float64,
		reflect.Bool:
		return reflect.ValueOf(v).Convert(t).Interface(), nil
	default:
		// time.Time
		if t == reflect.TypeOf(time.Time{}) {
			temp, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return nil, err
			}
			return temp, nil
		}

		// struct
		temp := reflect.New(t).Interface()
		if err := json.Unmarshal([]byte(v), temp); err != nil {
			return nil, err
		}
		return temp, nil
	}
}

// unmarshal value to the given kind or struct.
func unmarshal[T any](v string) (result *T, err error) {
	var t T
	tType := reflect.TypeOf(t)
	switch tType.Kind() {
	case reflect.String,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Float32,
		reflect.Float64,
		reflect.Bool:
		t = reflect.ValueOf(v).Convert(tType).Interface().(T)
		return &t, nil
	default:
		// time.Time
		if tType == reflect.TypeOf(time.Time{}) {
			temp, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return nil, err
			}
			t = reflect.ValueOf(temp).Convert(tType).Interface().(T)
			return &t, nil
		}

		// struct
		if err = json.Unmarshal([]byte(v), &t); err != nil {
			return nil, err
		}
		return &t, err
	}
}
