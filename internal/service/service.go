package service

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/teltech/logger"
	"github.com/zpiroux/geist/internal/pkg/assembly"
	"github.com/zpiroux/geist/internal/pkg/engine"
	"github.com/zpiroux/geist/internal/pkg/registry"
)

var log *logger.Log

func init() {
	log = logger.New()
}

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
	Registry        registry.Config
	Engine          engine.Config
	Entity          assembly.Config
}

// TODO: remove log and change to return err
func (c Config) Close() {
	if err := c.Entity.Close(); err != nil {
		log.Errorf("error closing ETL entities: %v", err)
	}
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

func (s *Service) Run(ctx context.Context) error {
	return s.supervisor.Run(ctx, &s.ready)
}

func (s *Service) AwaitReady() {
	s.ready.Wait()
}

func (s *Service) Shutdown(ctx context.Context, err error) {
	s.config.Close()
	s.supervisor.Shutdown(err)
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

func (s *Service) Entities() map[string]map[string]bool {
	return s.entityFactory.Entities()
}

func (s *Service) String() string {
	b, _ := json.Marshal(&s.config)
	return string(b)
}
