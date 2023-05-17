package goredis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/cache/v8"
	json "github.com/goccy/go-json"
	"github.com/hecigo/goutils"
)

var caches map[string]*cache.Cache = make(map[string]*cache.Cache)
var cacheConfigs map[string]*CacheConfig = make(map[string]*CacheConfig)

type CacheConfig struct {
	// Number of keys to cache in-process with TinyLFU algorith.
	// Set by env `REDIS_CACHE_TINYFLU_SIZE`.
	// Default: 10000
	TinyFLUSize int

	// Cache duration of TinyFLU algorithm
	// Set by env `REDIS_CACHE_TINYFLU_DURATION`.
	// Default: 1m
	TinyFLUDuration time.Duration

	// Default cache expiration time in Redis
	// Set by env `REDIS_CACHE_TTL`.
	// Default: 15m
	DefaultTTL time.Duration
}

// Create a cache instance with the given connection name. The default connection name is `cache`.
// We use TinyLFU as the default cache algorithm, [view more...]
//
// [view more...]: https://redis.uptrace.dev/guide/go-redis-cache.html#go-redis-cache
func EnableCache(name ...string) {
	if len(name) == 0 {
		name = append(name, "cache")
	}

	for _, connName := range name {

		// open the Redis connection
		Open(connName)

		// use local in-process storage to cache the small subset of popular keys
		// default cache 10,000 keys for 1 minute
		tinyFLUSize := goutils.Env(fmt.Sprintf("REDIS%s_TINYFLU_SIZE", connName), 10000)
		tinyFLUDuration := goutils.Env(fmt.Sprintf("REDIS%s_TINYFLU_DURATION", connName), time.Minute)
		ttl := goutils.Env(fmt.Sprintf("REDIS%s_TTL", connName), 15*time.Minute)
		cacheConfigs[connName] = &CacheConfig{
			TinyFLUSize:     tinyFLUSize,
			TinyFLUDuration: tinyFLUDuration,
			DefaultTTL:      ttl,
		}

		ctx := context.WithValue(context.Background(), goutils.CtxKey_ConnName, connName)
		caches[connName] = cache.New(&cache.Options{
			Redis:      Client(ctx),
			LocalCache: cache.NewTinyLFU(tinyFLUSize, tinyFLUDuration),
			Marshal:    json.Marshal,
			Unmarshal:  json.Unmarshal,
		})

		// print the cache information
		goutils.Infof("───── RedisCache[%s]: enabled ─────\n", connName)
		goutils.Infof("REDIS%s_TINYFLU_SIZE: %d\n", connName, tinyFLUSize)
		goutils.Infof("REDIS%s_TINYFLU_DURATION: %s\n", connName, tinyFLUDuration)
		goutils.Infof("REDIS%s_TTL: %s\n", connName, ttl)
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

	connName := ctx[0].Value(goutils.CtxKey_ConnName)
	if connName == nil || connName == "" {
		return caches["cache"]
	}

	if caches[connName.(string)] == nil {
		goutils.Panicf("Redis cache `%s` is not enabled", connName)
	}

	return caches[connName.(string)]
}

func getCacheConfig(ctx ...context.Context) *CacheConfig {
	if len(cacheConfigs) == 0 {
		goutils.Panic("Redis cache is not enabled")
	}

	if len(ctx) == 0 {
		return cacheConfigs["cache"]
	}

	connName := ctx[0].Value(goutils.CtxKey_ConnName)
	if connName == nil || connName == "" {
		return cacheConfigs["cache"]
	}

	if cacheConfigs[connName.(string)] == nil {
		goutils.Panicf("Redis cache `%s` is not enabled", connName)
	}

	return cacheConfigs[connName.(string)]
}

// Get the value from the cache. The value must be a pointer.
func GetCache(ctx context.Context, key string, value interface{}) error {
	return Cache(ctx).Get(ctx, key, value)
}

// Set the value to the cache. If TTL is not provided, [DefaultTTL] will be used from env.
func SetCache(ctx context.Context, key string, value interface{}, TTL ...time.Duration) error {
	var ttl time.Duration
	if len(TTL) == 0 {
		ttl = getCacheConfig(ctx).DefaultTTL
	} else {
		ttl = TTL[0]
	}

	return Cache(ctx).Set(&cache.Item{
		Ctx:   ctx,
		Key:   key,
		Value: value,
		TTL:   ttl,
	})
}
