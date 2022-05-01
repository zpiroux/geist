package service

import (
	"context"
	"encoding/json"

	"github.com/teltech/logger"
	"github.com/zpiroux/geist/internal/pkg/assembly"
	"github.com/zpiroux/geist/internal/pkg/engine"
	"github.com/zpiroux/geist/internal/pkg/registry"
)

var log *logger.Log

func init() {
	log = logger.New()
}

// Service is responsible for creating and injecting concrete implementations of the various parts required by GEIST to function.
type Service struct {
	config        Config
	entityFactory *assembly.StreamEntityFactory
	registry      *registry.StreamRegistry
	streamBuilder *engine.StreamBuilder
	supervisor    *engine.Supervisor
}

type Config struct {
	ProjectId       string // TODO: Change to be part of a GCP config
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

	for {

		if err = s.initConfig(cfg); err != nil {
			break
		}

		if err = s.initGcpServices(ctx); err != nil {
			break
		}

		s.initengine()

		if err = s.initRegistry(ctx); err != nil {
			break
		}

		if err = s.initSupervisor(ctx); err != nil {
			break
		}

		break
	}

	return &s, err
}

func (s *Service) Run(ctx context.Context) error {
	return s.supervisor.Run(ctx)
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
	return stream.(*engine.Stream), err
}

func (s *Service) String() string {
	b, _ := json.Marshal(&s.config)
	return string(b)
}
