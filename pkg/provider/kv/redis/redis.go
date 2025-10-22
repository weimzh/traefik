package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/kvtools/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/provider"
	"github.com/traefik/traefik/v3/pkg/provider/kv"
	"github.com/traefik/traefik/v3/pkg/types"
)

var _ provider.Provider = (*Provider)(nil)

// Provider holds configurations of the provider.
type Provider struct {
	kv.Provider `yaml:",inline" export:"true"`

	TLS      *types.ClientTLS `description:"Enable TLS support." json:"tls,omitempty" toml:"tls,omitempty" yaml:"tls,omitempty" export:"true"`
	Username string           `description:"Username for authentication." json:"username,omitempty" toml:"username,omitempty" yaml:"username,omitempty" loggable:"false"`
	Password string           `description:"Password for authentication." json:"password,omitempty" toml:"password,omitempty" yaml:"password,omitempty" loggable:"false"`
	DB       int              `description:"Database to be selected after connecting to the server." json:"db,omitempty" toml:"db,omitempty" yaml:"db,omitempty"`
	Sentinel *Sentinel        `description:"Enable Sentinel support." json:"sentinel,omitempty" toml:"sentinel,omitempty" yaml:"sentinel,omitempty" export:"true"`

	// Direct Redis client for keyspace notifications
	redisClient goredis.UniversalClient
}

// Sentinel holds the Redis Sentinel configuration.
type Sentinel struct {
	MasterName string `description:"Name of the master." json:"masterName,omitempty" toml:"masterName,omitempty" yaml:"masterName,omitempty" export:"true"`
	Username   string `description:"Username for Sentinel authentication." json:"username,omitempty" toml:"username,omitempty" yaml:"username,omitempty" loggable:"false"`
	Password   string `description:"Password for Sentinel authentication." json:"password,omitempty" toml:"password,omitempty" yaml:"password,omitempty" loggable:"false"`

	LatencyStrategy bool `description:"Defines whether to route commands to the closest master or replica nodes (mutually exclusive with RandomStrategy and ReplicaStrategy)." json:"latencyStrategy,omitempty" toml:"latencyStrategy,omitempty" yaml:"latencyStrategy,omitempty" export:"true"`
	RandomStrategy  bool `description:"Defines whether to route commands randomly to master or replica nodes (mutually exclusive with LatencyStrategy and ReplicaStrategy)." json:"randomStrategy,omitempty" toml:"randomStrategy,omitempty" yaml:"randomStrategy,omitempty" export:"true"`
	ReplicaStrategy bool `description:"Defines whether to route all commands to replica nodes (mutually exclusive with LatencyStrategy and RandomStrategy)." json:"replicaStrategy,omitempty" toml:"replicaStrategy,omitempty" yaml:"replicaStrategy,omitempty" export:"true"`

	UseDisconnectedReplicas bool `description:"Use replicas disconnected with master when cannot get connected replicas." json:"useDisconnectedReplicas,omitempty" toml:"useDisconnectedReplicas,omitempty" yaml:"useDisconnectedReplicas,omitempty" export:"true"`
}

// SetDefaults sets the default values.
func (p *Provider) SetDefaults() {
	p.Provider.SetDefaults()
	p.Endpoints = []string{"127.0.0.1:6379"}
}

// Init the provider.
func (p *Provider) Init() error {
	config := &redis.Config{
		Username: p.Username,
		Password: p.Password,
		DB:       p.DB,
	}

	if p.TLS != nil {
		var err error
		config.TLS, err = p.TLS.CreateTLSConfig(context.Background())
		if err != nil {
			return fmt.Errorf("unable to create client TLS configuration: %w", err)
		}
	}

	if p.Sentinel != nil {
		switch {
		case p.Sentinel.LatencyStrategy && !(p.Sentinel.RandomStrategy || p.Sentinel.ReplicaStrategy):
		case p.Sentinel.RandomStrategy && !(p.Sentinel.LatencyStrategy || p.Sentinel.ReplicaStrategy):
		case p.Sentinel.ReplicaStrategy && !(p.Sentinel.RandomStrategy || p.Sentinel.LatencyStrategy):
			return errors.New("latencyStrategy, randomStrategy and replicaStrategy options are mutually exclusive, please use only one of those options")
		}

		clusterClient := p.Sentinel.LatencyStrategy || p.Sentinel.RandomStrategy
		config.Sentinel = &redis.Sentinel{
			MasterName:              p.Sentinel.MasterName,
			Username:                p.Sentinel.Username,
			Password:                p.Sentinel.Password,
			ClusterClient:           clusterClient,
			RouteByLatency:          p.Sentinel.LatencyStrategy,
			RouteRandomly:           p.Sentinel.RandomStrategy,
			ReplicaOnly:             p.Sentinel.ReplicaStrategy,
			UseDisconnectedReplicas: p.Sentinel.UseDisconnectedReplicas,
		}

		// Create Sentinel client for keyspace notifications
		p.redisClient = goredis.NewFailoverClient(&goredis.FailoverOptions{
			MasterName:       p.Sentinel.MasterName,
			SentinelAddrs:    p.Endpoints,
			SentinelUsername: p.Sentinel.Username,
			SentinelPassword: p.Sentinel.Password,
			Username:         p.Username,
			Password:         p.Password,
			DB:               p.DB,
			TLSConfig:        config.TLS,
		})
	} else {
		// Create standard Redis client for keyspace notifications
		if len(p.Endpoints) == 0 {
			p.Endpoints = []string{"127.0.0.1:6379"}
		}

		p.redisClient = goredis.NewClient(&goredis.Options{
			Addr:      p.Endpoints[0],
			Username:  p.Username,
			Password:  p.Password,
			DB:        p.DB,
			TLSConfig: config.TLS,
		})
	}

	// Test the direct Redis connection
	ctx := context.Background()
	if err := p.redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis for keyspace notifications: %w", err)
	}

	// Set the outer provider reference so kv.Provider can detect GranularWatcher implementation
	p.Provider.SetOuterProvider(p)

	return p.Provider.Init(redis.StoreName, "redis", config)
}

// WatchKeys watches for specific key changes using Redis keyspace notifications.
// This implements the kv.GranularWatcher interface for incremental updates.
func (p *Provider) WatchKeys(ctx context.Context, prefix string) (<-chan kv.KeyChangeEvent, error) {
	events := make(chan kv.KeyChangeEvent, 100)

	// Subscribe to keyspace notifications for the prefix
	// Pattern: __keyspace@{db}__:{prefix}*
	pattern := fmt.Sprintf("__keyspace@%d__:%s*", p.DB, prefix)

	logger := log.Ctx(ctx).With().
		Str("pattern", pattern).
		Int("db", p.DB).
		Logger()

	logger.Info().Msg("Subscribing to Redis keyspace notifications")

	pubsub := p.redisClient.PSubscribe(ctx, pattern)

	go func() {
		defer close(events)
		defer func() {
			if err := pubsub.Close(); err != nil {
				logger.Error().Err(err).Msg("Error closing pubsub")
			}
		}()

		ch := pubsub.Channel()
		logger.Info().Msg("Listening for Redis keyspace events")

		for {
			select {
			case <-ctx.Done():
				logger.Info().Msg("Stopping Redis keyspace watch")
				return
			case msg, ok := <-ch:
				if !ok {
					logger.Warn().Msg("Redis pubsub channel closed")
					return
				}

				// Parse keyspace notification
				// Channel format: __keyspace@{db}__:{key}
				// Message: "set", "del", "expire", "expired", etc.
				key := extractKeyFromChannel(msg.Channel, p.DB)
				operation := msg.Payload

				eventLogger := logger.With().
					Str("key", key).
					Str("operation", operation).
					Logger()

				var value []byte
				if operation == "set" || operation == "hset" {
					// Fetch the new value for set operations
					val, err := p.redisClient.Get(ctx, key).Result()
					if err != nil {
						if err != goredis.Nil {
							eventLogger.Error().Err(err).Msg("Failed to get key value")
						}
						continue
					}
					value = []byte(val)
				}

				eventLogger.Debug().Msg("Received keyspace event")

				select {
				case events <- kv.KeyChangeEvent{
					Key:       key,
					Operation: operation,
					Value:     value,
				}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return events, nil
}

// extractKeyFromChannel extracts the key name from a Redis keyspace notification channel.
// Channel format: __keyspace@{db}__:{key}
func extractKeyFromChannel(channel string, db int) string {
	prefix := fmt.Sprintf("__keyspace@%d__:", db)
	return strings.TrimPrefix(channel, prefix)
}
