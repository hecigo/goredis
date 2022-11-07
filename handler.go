package goredis

import (
	"context"
	"reflect"
	"time"

	"github.com/go-redis/redis/v8"
	json "github.com/goccy/go-json"
)

func genKey(ctx context.Context, key string) string {
	cfg := GetConfig(ctx)
	return cfg.KeyPrefix + "." + key
}

// Get value of key and unmarshal it to the given kind or struct.
//
//   - [T] can be a kind of value or a struct. If T is a struct, the value will be unmarshaled to it.
//   - [ctx] includes [goutils.CtxConnNameKey] to specify the connection name.
//   - [key] is the key to get without prefix.
func Get[T any](ctx context.Context, key string) (*T, error) {
	val, err := Client(ctx).Get(ctx, genKey(ctx, key)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var t T
	switch reflect.TypeOf(t).Kind() {
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
		t = reflect.ValueOf(val).Convert(reflect.TypeOf(t)).Interface().(T)
		return &t, nil
	default:
		if reflect.TypeOf(t) == reflect.TypeOf(time.Time{}) {
			temp, err := time.Parse(time.RFC3339, val)
			if err != nil {
				return nil, err
			}
			t = reflect.ValueOf(temp).Convert(reflect.TypeOf(t)).Interface().(T)
			return &t, nil
		}
		if err = json.Unmarshal([]byte(val), &t); err != nil {
			return nil, err
		}
		return &t, err
	}
}
