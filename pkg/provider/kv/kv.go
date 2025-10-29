package kv

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/config/kv"
	"github.com/traefik/traefik/v3/pkg/job"
	"github.com/traefik/traefik/v3/pkg/observability/logs"
	"github.com/traefik/traefik/v3/pkg/safe"
)

// GranularWatcher allows providers to receive specific key change events.
// Providers that implement this interface will use incremental updates
// instead of full scans on every change.
type GranularWatcher interface {
	WatchKeys(ctx context.Context, prefix string) (<-chan KeyChangeEvent, error)
}

// KeyChangeEvent represents a single key change in the KV store.
type KeyChangeEvent struct {
	Key       string // Full key path
	Operation string // "set", "del", "expire"
	Value     []byte // New value (nil for delete operations)
}

// Provider holds configurations of the provider.
type Provider struct {
	RootKey string `description:"Root key used for KV store." json:"rootKey,omitempty" toml:"rootKey,omitempty" yaml:"rootKey,omitempty"`

	Endpoints []string `description:"KV store endpoints." json:"endpoints,omitempty" toml:"endpoints,omitempty" yaml:"endpoints,omitempty"`

	name     string
	kvClient store.Store

	// Internal state management for incremental updates
	mu            sync.RWMutex
	currentConfig *dynamic.Configuration
	kvPairs       map[string]string
	lastFullScan  time.Time

	// Reference to the outer provider that may implement GranularWatcher
	outerProvider interface{}
}

// SetDefaults sets the default values.
func (p *Provider) SetDefaults() {
	p.RootKey = "traefik"
}

// SetOuterProvider sets the reference to the outer provider that may implement GranularWatcher.
func (p *Provider) SetOuterProvider(outer interface{}) {
	p.outerProvider = outer
}

// Init the provider.
func (p *Provider) Init(storeType, name string, config valkeyrie.Config) error {
	ctx := log.With().Str(logs.ProviderName, name).Logger().WithContext(context.Background())

	p.name = name

	kvClient, err := p.createKVClient(ctx, storeType, config)
	if err != nil {
		return fmt.Errorf("failed to Connect to KV store: %w", err)
	}

	p.kvClient = kvClient

	return nil
}

// Provide allows the docker provider to provide configurations to traefik using the given configuration channel.
func (p *Provider) Provide(configurationChan chan<- dynamic.Message, pool *safe.Pool) error {
	logger := log.With().Str(logs.ProviderName, p.name).Logger()
	ctx := logger.WithContext(context.Background())

	operation := func() error {
		if _, err := p.kvClient.Exists(ctx, path.Join(p.RootKey, "qmslkjdfmqlskdjfmqlksjazcueznbvbwzlkajzebvkwjdcqmlsfj"), nil); err != nil {
			return fmt.Errorf("KV store connection error: %w", err)
		}
		return nil
	}

	notify := func(err error, time time.Duration) {
		logger.Error().Err(err).Msgf("KV connection error, retrying in %s", time)
	}

	err := backoff.RetryNotify(safe.OperationWithRecover(operation), backoff.WithContext(job.NewBackOff(backoff.NewExponentialBackOff()), ctx), notify)
	if err != nil {
		return fmt.Errorf("cannot connect to KV server: %w", err)
	}

	configuration, err := p.buildConfiguration(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("Cannot build the configuration")
	} else {
		configurationChan <- dynamic.Message{
			ProviderName:  p.name,
			Configuration: configuration,
		}
	}

	pool.GoCtx(func(ctxPool context.Context) {
		ctxLog := logger.With().Str(logs.ProviderName, p.name).Logger().WithContext(ctxPool)

		err := p.watchKv(ctxLog, configurationChan)
		if err != nil {
			logger.Error().Err(err).Msg("Cannot retrieve data")
		}
	})

	return nil
}

func (p *Provider) watchKv(ctx context.Context, configurationChan chan<- dynamic.Message) error {
	// Check if outer provider supports granular watching
	if p.outerProvider != nil {
		if granularWatcher, ok := p.outerProvider.(GranularWatcher); ok {
			log.Ctx(ctx).Info().Msg("Using incremental update mode")
			return p.watchKvGranular(ctx, configurationChan, granularWatcher)
		}
	}

	// Fallback to full scan mode for providers that don't support granular watching
	log.Ctx(ctx).Info().Msg("Using full scan mode")
	return p.watchKvFullScan(ctx, configurationChan)
}

func (p *Provider) watchKvFullScan(ctx context.Context, configurationChan chan<- dynamic.Message) error {
	operation := func() error {
		events, err := p.kvClient.WatchTree(ctx, p.RootKey, nil)
		if err != nil {
			return fmt.Errorf("failed to watch KV: %w", err)
		}

		for {
			select {
			case <-ctx.Done():
				return nil
			case _, ok := <-events:
				if !ok {
					return errors.New("the WatchTree channel is closed")
				}

				configuration, errC := p.buildConfiguration(ctx)
				if errC != nil {
					return errC
				}

				if configuration != nil {
					configurationChan <- dynamic.Message{
						ProviderName:  p.name,
						Configuration: configuration,
					}
				}
			}
		}
	}

	notify := func(err error, time time.Duration) {
		log.Ctx(ctx).Error().Err(err).Msgf("Provider error, retrying in %s", time)
	}

	return backoff.RetryNotify(safe.OperationWithRecover(operation),
		backoff.WithContext(job.NewBackOff(backoff.NewExponentialBackOff()), ctx), notify)
}

func (p *Provider) watchKvGranular(ctx context.Context, configurationChan chan<- dynamic.Message, watcher GranularWatcher) error {
	// Initialize state with full load on startup
	p.mu.Lock()
	if p.kvPairs == nil {
		pairs, err := p.kvClient.List(ctx, p.RootKey, nil)
		if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
			p.mu.Unlock()
			return err
		}

		p.kvPairs = make(map[string]string)
		if pairs != nil {
			for _, pair := range pairs {
				p.kvPairs[pair.Key] = string(pair.Value)
			}
		}
		p.lastFullScan = time.Now()
		log.Ctx(ctx).Info().Int("keys", len(p.kvPairs)).Msg("Initial state loaded")
	}
	p.mu.Unlock()

	operation := func() error {
		// Subscribe to key changes
		events, err := watcher.WatchKeys(ctx, p.RootKey)
		if err != nil {
			return fmt.Errorf("failed to watch keys: %w", err)
		}

		for {
			select {
			case <-ctx.Done():
				return nil

			case event, ok := <-events:
				if !ok {
					return errors.New("watch channel closed")
				}

				// Check if 10 minutes have passed since last full scan
				p.mu.RLock()
				timeSinceLastScan := time.Since(p.lastFullScan)
				p.mu.RUnlock()

				var configuration *dynamic.Configuration

				if timeSinceLastScan >= 10*time.Minute {
					// Perform full scan (event-triggered periodic scan)
					log.Ctx(ctx).Info().
						Str("timeSinceLastScan", timeSinceLastScan.String()).
						Msg("Performing event-triggered full scan")
					configuration = p.performFullScan(ctx, events)
				} else {
					// Apply incremental change
					configuration = p.applyKeyChange(ctx, event)
				}

				if configuration != nil {
					configurationChan <- dynamic.Message{
						ProviderName:  p.name,
						Configuration: configuration,
					}
				}
			}
		}
	}

	notify := func(err error, time time.Duration) {
		log.Ctx(ctx).Error().Err(err).Msgf("Provider error, retrying in %s", time)
	}

	return backoff.RetryNotify(safe.OperationWithRecover(operation),
		backoff.WithContext(job.NewBackOff(backoff.NewExponentialBackOff()), ctx), notify)
}

func (p *Provider) performFullScan(ctx context.Context, events <-chan KeyChangeEvent) *dynamic.Configuration {
	logger := log.Ctx(ctx)

	// Fetch all keys from the KV store (without holding the lock during I/O)
	pairs, err := p.kvClient.List(ctx, p.RootKey, nil)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			// No keys found - clear internal state
			p.mu.Lock()
			defer p.mu.Unlock()
			if len(p.kvPairs) > 0 {
				logger.Info().Msg("All keys removed, clearing state")
				p.kvPairs = make(map[string]string)
				return &dynamic.Configuration{
					HTTP: &dynamic.HTTPConfiguration{
						Routers: make(map[string]*dynamic.Router),
					},
				}
			}
			return nil
		}
		logger.Error().Err(err).Msg("Failed to list keys during full scan")
		return nil
	}

	// Build map of current keys from Redis
	currentKeys := make(map[string]string)
	for _, pair := range pairs {
		currentKeys[pair.Key] = string(pair.Value)
	}

	// Now acquire the lock and drain any pending events that arrived during List()
	// This ensures we apply the most recent state
	p.mu.Lock()
	defer p.mu.Unlock()

	drained := 0
	for {
		select {
		case event, ok := <-events:
			if !ok {
				// Channel closed, stop draining
				logger.Warn().Msg("Event channel closed during full scan")
				goto drainComplete
			}
			// Apply the event to currentKeys (not kvPairs yet)
			switch event.Operation {
			case "set":
				currentKeys[event.Key] = string(event.Value)
				drained++
			case "del", "expire", "expired":
				delete(currentKeys, event.Key)
				drained++
			}
		default:
			// No more events in channel, we're done
			goto drainComplete
		}
	}

drainComplete:
	if drained > 0 {
		logger.Info().Int("events", drained).Msg("Applied pending events to full scan result")
	}

	// Detect changes
	hasChanges := false
	added := 0
	modified := 0
	deleted := 0

	// Check for additions and modifications
	for key, value := range currentKeys {
		if oldValue, exists := p.kvPairs[key]; !exists {
			added++
			hasChanges = true
		} else if oldValue != value {
			modified++
			hasChanges = true
		}
	}

	// Check for deletions
	for key := range p.kvPairs {
		if _, exists := currentKeys[key]; !exists {
			deleted++
			hasChanges = true
		}
	}

	if !hasChanges {
		logger.Debug().Msg("Full scan: no changes detected")
		return nil
	}

	logger.Info().
		Int("added", added).
		Int("modified", modified).
		Int("deleted", deleted).
		Int("total", len(currentKeys)).
		Msg("Full scan: changes detected")

	// Update internal state
	p.kvPairs = currentKeys
	p.lastFullScan = time.Now()

	// Decode configuration
	cfg := &dynamic.Configuration{}
	if err := kv.Decode(pairs, cfg, p.RootKey); err != nil {
		logger.Error().Err(err).Msg("Failed to decode configuration during full scan")
		return nil
	}

	p.currentConfig = cfg
	return cfg
}

func (p *Provider) applyKeyChange(ctx context.Context, event KeyChangeEvent) *dynamic.Configuration {
	p.mu.Lock()
	defer p.mu.Unlock()

	changed := false
	logger := log.Ctx(ctx).With().
		Str("key", event.Key).
		Str("operation", event.Operation).
		Logger()

	switch event.Operation {
	case "set":
		newValue := string(event.Value)
		if oldValue, exists := p.kvPairs[event.Key]; !exists || oldValue != newValue {
			p.kvPairs[event.Key] = newValue
			changed = true
			logger.Debug().Msg("Key updated")
		} else {
			logger.Debug().Msg("Key unchanged, skipping")
		}
	case "del", "expire", "expired":
		if _, exists := p.kvPairs[event.Key]; exists {
			delete(p.kvPairs, event.Key)
			changed = true
			logger.Debug().Msg("Key deleted")
		} else {
			logger.Debug().Msg("Key already deleted, skipping")
		}
	default:
		logger.Debug().Msg("Ignoring operation")
	}

	if !changed {
		return nil
	}

	// Rebuild configuration from current state
	pairs := make([]*store.KVPair, 0, len(p.kvPairs))
	for key, value := range p.kvPairs {
		pairs = append(pairs, &store.KVPair{
			Key:   key,
			Value: []byte(value),
		})
	}

	cfg := &dynamic.Configuration{}
	if err := kv.Decode(pairs, cfg, p.RootKey); err != nil {
		logger.Error().Err(err).Msg("Failed to decode configuration")
		return nil
	}

	p.currentConfig = cfg
	logger.Info().Int("totalKeys", len(p.kvPairs)).Msg("Configuration updated")
	return cfg
}

func (p *Provider) buildConfiguration(ctx context.Context) (*dynamic.Configuration, error) {
	pairs, err := p.kvClient.List(ctx, p.RootKey, nil)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			// This empty configuration satisfies the pkg/server/configurationwatcher.go isEmptyConfiguration func constraints,
			// and will not be discarded by the configuration watcher.
			return &dynamic.Configuration{
				HTTP: &dynamic.HTTPConfiguration{
					Routers: make(map[string]*dynamic.Router),
				},
			}, nil
		}

		return nil, err
	}

	cfg := &dynamic.Configuration{}
	err = kv.Decode(pairs, cfg, p.RootKey)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func (p *Provider) createKVClient(ctx context.Context, storeType string, config valkeyrie.Config) (store.Store, error) {
	kvStore, err := valkeyrie.NewStore(ctx, storeType, p.Endpoints, config)
	if err != nil {
		return nil, err
	}

	return &storeWrapper{Store: kvStore}, nil
}
