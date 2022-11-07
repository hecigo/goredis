package goredis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/cache/v8"
	json "github.com/goccy/go-json"
	"github.com/hpi-tech/goutils"
)

var caches map[string]*cache.Cache = make(map[string]*cache.Cache)

// Create a cache instance with the given connection name. The default connection name is `cache`.
// We use TinyLFU as the default cache algorithm, including the following options:
// - REDIS_TINYFLU_SIZE: the maximum number of keys to in-process cache, default 10000
// - REDIS_TINYFLU_DURATION: the duration to cache the keys, default 1m
func EnableCache(name ...string) {
	if len(name) == 0 {
		name = append(name, "cache")
	}

	for _, connName := range name {

		// open the Redis connection
		Open(connName)

		// use local in-process storage to cache the small subset of popular keys
		// default cache 10,000 keys for 1 minute
		size := goutils.Env(fmt.Sprintf("REDIS%s_TINYFLU_SIZE", connName), 10000)
		duration := goutils.Env(fmt.Sprintf("REDIS%s_TINYFLU_DURATION", connName), time.Minute)

		ctx := context.WithValue(context.Background(), goutils.CtxConnNameKey, connName)
		caches[connName] = cache.New(&cache.Options{
			Redis:      Client(ctx),
			LocalCache: cache.NewTinyLFU(size, duration),
			Marshal:    json.Marshal,
			Unmarshal:  json.Unmarshal,
		})

		// print the cache information
		goutils.Infof("───── RedisCache[%s]: enabled ─────\n", connName)
		goutils.Infof("REDIS%s_TINYFLU_SIZE: %d\n", connName, size)
		goutils.Infof("REDIS%s_TINYFLU_DURATION: %s\n", connName, duration)
		goutils.Info("───────────────────────────────────\n")
	}
}

// Get the cache instance with the given connection name. If context is not provided, the default cache will be used.
func Cache(ctx ...context.Context) *cache.Cache {
	if len(caches) == 0 {
		goutils.Panic("Redis cache is not enabled")
	}

	if len(ctx) == 0 {
		return caches["cache"]
	}

	connName := ctx[0].Value(goutils.CtxConnNameKey)
	if connName == nil || connName == "" {
		return caches["cache"]
	}

	if caches[connName.(string)] == nil {
		goutils.Panicf("Redis cache `%s` is not enabled", connName)
	}

	return caches[connName.(string)]
}
