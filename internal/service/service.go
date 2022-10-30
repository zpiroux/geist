package service

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/internal/pkg/assembly"
	"github.com/zpiroux/geist/internal/pkg/engine"
	"github.com/zpiroux/geist/internal/pkg/registry"
)

// Service is responsible for creating and injecting concrete implementations
// of the various parts required by GEIST to function.
type Service struct {
	config        Config
	entityFactory *assembly.StreamEntityFactory
	registry      *registry.StreamRegistry
	streamBuilder *engine.StreamBuilder
	supervisor    *engine.Supervisor
	ready         sync.WaitGroup
}

type Config struct {
	AdminStreamSpec []byte
	NotifyChanSize  int
	Registry        registry.Config
	Engine          engine.Config
	Entity          assembly.Config
}

func (c Config) Close(ctx context.Context) error {
	return c.Entity.Close(ctx)
}

func New(ctx context.Context, cfg Config) (*Service, error) {

	var (
		s   Service
		err error
	)

	if err = s.initConfig(cfg); err != nil {
		return &s, err
	}

	s.initengine()

	if err = s.initRegistry(ctx); err != nil {
		return &s, err
	}

	err = s.initSupervisor(ctx)

	// Set the amount of participants to wait for before allowing further use of GEIST API
	// after starting it up with Run.
	// Currently we'll only be waiting for Supervisor to complete its startup process in
	// Supervisor.Run().
	s.ready.Add(1)

	return &s, err
}

func (s *Service) NotifyChan() entity.NotifyChan {
	return s.config.Engine.NotifyChan
}

func (s *Service) Run(ctx context.Context) error {
	return s.supervisor.Run(ctx, &s.ready)
}

func (s *Service) AwaitReady() {
	s.ready.Wait()
}

func (s *Service) Shutdown(ctx context.Context) error {
	err := s.config.Close(ctx)
	s.supervisor.Shutdown(ctx)
	return err
}

func (s *Service) Registry() *registry.StreamRegistry {
	return s.registry
}

func (s *Service) Stream(streamId string) (*engine.Stream, error) {
	stream, err := s.supervisor.Stream(streamId)
	if err != nil {
		return nil, err
	}
	return stream.(*engine.Stream), err
}

func (s *Service) Metrics() map[string]entity.Metrics {
	return s.supervisor.Metrics()
}

func (s *Service) Entities() map[string]map[string]bool {
	return s.entityFactory.Entities()
}

func (s *Service) String() string {
	b, _ := json.Marshal(&s.config)
	return string(b)
}
