// `goredis` is a Go client for Redis. It's implemented in pure Go and supports
// all Redis commands. It's compatible with Redis 6 and higher.
//
// `goredis` is a fork of [go-redis] with some changes to make it easier to use.
//
// [go-redis]: https://github.com/go-redis/redis
package goredis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hpi-tech/goutils"
	apmgoredis "go.elastic.co/apm/module/apmgoredisv8/v2"
)

// Configuration options
type Config struct {
	// The connection name. It is used to get the connection information from the .env file.
	// Default: default
	ConnectionName string

	// The address of the Redis server, split by dot-comma (e.g. localhost:6379;localhost:6380).
	// Default: localhost:6379
	Addresses []string

	// The base authentication to login to the Redis server.
	// Include the username and password, split by colon (e.g. username:password)
	// Default: ":", which means no authentication (empty username and password).
	BasicAuth []string

	// The default database to use.
	// Default: 0
	DB int

	// The dial timeout for establishing new connections.
	// Default: 1s
	DialTimeout time.Duration

	// The read timeout for commands.
	// Default: 3s
	ReadTimeout time.Duration

	// The write timeout for commands.
	// Default: 3s
	WriteTimeout time.Duration

	// The name of master node in Redis Sentinel.
	// Default: ""
	MasterName string

	// Connection pool size for extra read/write connections.
	// Read more: https://redis.uptrace.dev/guide/go-redis-debugging.html#connection-pool-size
	// Default: 10
	PoolSize int

	// The maximum number of retries before giving up.
	// Default: 3
	MaxRetries int

	// The prefix for all keys.
	// Default: ""
	KeyPrefix string
}

var (
	clients map[string]redis.UniversalClient = make(map[string]redis.UniversalClient)
	configs map[string]*Config               = make(map[string]*Config)
)

// Open a Redis connection with name. If name is not provided, the default connection will be used.
// This function creates only [redis.UniversalClient].
//
// [redis.UniversalClient]: https://redis.uptrace.dev/guide/universal.html
func Open(name ...string) error {
	// append the default connection name if the name is empty
	if len(name) == 0 {
		name = append(name, "")
	}

	// get the connection information from the .env file
	for _, connName := range name {

		// create default configuration
		cfg := Config{
			Addresses:    []string{"localhost:6379"},
			BasicAuth:    []string{"", ""},
			DB:           0,
			DialTimeout:  1 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			MasterName:   "",
			PoolSize:     10,
			MaxRetries:   3,
			KeyPrefix:    "",
		}

		// set connection name
		if connName == "" {
			cfg.ConnectionName = "default"
		} else {
			cfg.ConnectionName = connName
		}

		// override the default configuration if the connection name is not default

		addresses := strings.Split(goutils.Env(fmt.Sprintf("REDIS%s_URL", connName), ""), ";")
		if len(addresses) > 0 && addresses[0] != "" {
			cfg.Addresses = addresses
		}

		basicAuth := strings.Split(goutils.Env(fmt.Sprintf("REDIS%s_BASIC_AUTH", connName), ":"), ":")
		if len(basicAuth) > 0 && basicAuth[0] != "" {
			cfg.BasicAuth = basicAuth
		}

		db := goutils.Env(fmt.Sprintf("REDIS%s_DB", connName), -1)
		if db >= 0 {
			cfg.DB = db
		}

		dialTimeout := goutils.Env(fmt.Sprintf("REDIS%s_DIAL_TIMEOUT", connName), 0*time.Second)
		if dialTimeout > 0 {
			cfg.DialTimeout = dialTimeout
		}

		readTimeout := goutils.Env(fmt.Sprintf("REDIS%s_READ_TIMEOUT", connName), 0*time.Second)
		if readTimeout > 0 {
			cfg.ReadTimeout = readTimeout
		}

		writeTimeout := goutils.Env(fmt.Sprintf("REDIS%s_WRITE_TIMEOUT", connName), 0*time.Second)
		if writeTimeout > 0 {
			cfg.WriteTimeout = writeTimeout
		}

		masterName := goutils.Env(fmt.Sprintf("REDIS%s_MASTER_NAME", connName), "")
		if masterName != "" {
			cfg.MasterName = masterName
		}

		poolSize := goutils.Env(fmt.Sprintf("REDIS%s_POOL_SIZE", connName), 0)
		if poolSize > 0 {
			cfg.PoolSize = poolSize
		}

		maxRetries := goutils.Env(fmt.Sprintf("REDIS%s_MAX_RETRIES", connName), 0)
		if maxRetries > 0 {
			cfg.MaxRetries = maxRetries
		}

		keyPrefix := goutils.Env(fmt.Sprintf("REDIS%s_KEY_PREFIX", connName), "")
		if keyPrefix != "" {
			cfg.KeyPrefix = keyPrefix
		} else {
			cfg.KeyPrefix = goutils.ToURL(goutils.AppName())
		}
		if cfg.KeyPrefix == "" {
			goutils.Fatalf("REDIS%s_KEY_PREFIX must be set", connName)
		}

		// set the configuration
		configs[cfg.ConnectionName] = &cfg

		// create the Redis client
		client := redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:        cfg.Addresses,
			Username:     cfg.BasicAuth[0],
			Password:     cfg.BasicAuth[1],
			DB:           cfg.DB,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			MasterName:   cfg.MasterName,
			PoolSize:     cfg.PoolSize,
			MaxRetries:   cfg.MaxRetries,
		})

		// add APM hook
		if goutils.Env("ELASTIC_APM_ENABLE", true) {
			client.AddHook(apmgoredis.NewHook())
		}

		// set the Redis client
		if clients[cfg.ConnectionName] != nil {
			Close(cfg.ConnectionName)
		}
		clients[cfg.ConnectionName] = client

		// print the connection information
		Print(cfg.ConnectionName)
	}

	return nil
}

// Close a Redis connection with name. If name is not provided, the default connection will be closed.
func Close(name ...string) error {
	if len(name) == 0 {
		name = append(name, "default")
	}

	for _, connName := range name {
		if clients[connName] != nil {
			err := clients[connName].Close()
			if err != nil {
				return err
			}
			clients[connName] = nil
		}
		if configs[connName] != nil {
			configs[connName] = nil
		}
	}

	return nil
}

// Returns the Redis client with name. If name is not provided, the default connection will be returned.
func Client(ctx ...context.Context) redis.UniversalClient {
	if len(clients) == 0 {
		goutils.Fatal("Redis client is not initialized")
	}

	if len(ctx) == 0 || ctx[0] == nil {
		return clients["default"]
	}

	connName := ctx[0].Value(goutils.CtxKey_ConnName)
	if connName == nil || connName == "" {
		return clients["default"]
	}

	if clients[connName.(string)] == nil {
		goutils.Fatalf("Redis client `%s` is not initialized", connName)
	}

	return clients[connName.(string)]
}

// Returns the Redis configuration with name. If name is not provided, the default config will be returned.
func GetConfig(ctx ...context.Context) *Config {
	if len(configs) == 0 {
		return nil
	}

	if len(ctx) == 0 || ctx[0] == nil {
		return configs["default"]
	}

	connName := ctx[0].Value(goutils.CtxKey_ConnName)
	if connName == nil || connName == "" {
		return configs["default"]
	}

	return configs[connName.(string)]
}

// Print the Redis connection information with name. If name is not provided, the default connection will be printed.
func Print(name ...string) {
	if len(name) == 0 {
		name = append(name, "default")
	}

	for _, connName := range name {
		if clients[connName] != nil {
			goutils.Printf("───── Redis[%s]: opened ─────", connName)
			goutils.Printf("  Addresses: %s", configs[connName].Addresses)
			goutils.Printf("  BasicAuth: %s", configs[connName].BasicAuth)
			goutils.Printf("  DB: %d", configs[connName].DB)
			goutils.Printf("  DialTimeout: %s", configs[connName].DialTimeout)
			goutils.Printf("  ReadTimeout: %s", configs[connName].ReadTimeout)
			goutils.Printf("  WriteTimeout: %s", configs[connName].WriteTimeout)
			goutils.Printf("  MasterName: %s", configs[connName].MasterName)
			goutils.Printf("  PoolSize: %d", configs[connName].PoolSize)
			goutils.Printf("  MaxRetries: %d", configs[connName].MaxRetries)
			goutils.Printf("  KeyPrefix: %s", configs[connName].KeyPrefix)
			goutils.Print("───────────────────────────────")
		}
	}
}
