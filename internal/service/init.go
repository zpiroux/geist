package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/zpiroux/geist/internal/pkg/assembly"
	"github.com/zpiroux/geist/internal/pkg/engine"
	"github.com/zpiroux/geist/internal/pkg/model"
	"github.com/zpiroux/geist/internal/pkg/registry"
)

func (s *Service) initConfig(config Config) error {
	var err error
	s.config = config

	adminEventSpec := model.AdminEventSpecInMem

	switch config.Registry.StorageMode {
	case model.RegStorageNative:
		s.config.Registry.RegSpec = model.SpecRegistrationSpec
		adminEventSpec = model.AdminEventSpec
	case model.RegStorageInMemory:
		s.config.Registry.RegSpec = model.SpecRegistrationSpecInMem
	case model.RegStorageCustom:
		s.config.Registry.RegSpec = config.Registry.RegSpec
		adminEventSpec = model.AdminEventSpec
	default:
		s.config.Registry.StorageMode = model.RegStorageInMemory
		s.config.Registry.RegSpec = model.SpecRegistrationSpecInMem
	}

	// If admin spec provided, it overrides above defaults
	if config.AdminStreamSpec != nil {
		adminEventSpec = config.AdminStreamSpec
	}

	s.config.Engine.RegSpec, err = model.NewSpec(s.config.Registry.RegSpec)
	if err != nil {
		return fmt.Errorf("could not create Stream Spec for Registry, error: %s", err)
	}
	s.config.Engine.AdminSpec, err = model.NewSpec(adminEventSpec)
	if err != nil {
		err = fmt.Errorf("could not create Stream Spec for Admin events, error: %s", err)
	}
	return err
}

func (s *Service) initGcpServices(ctx context.Context) (err error) {

	if s.config.ProjectId == "" {
		return nil
	}

	if err = s.config.Entity.BigTable.InitClients(ctx, s.config.ProjectId); err != nil {
		return err
	}
	if err = s.config.Entity.BigQuery.InitClient(ctx, s.config.ProjectId); err != nil {
		return err
	}
	if err = s.config.Entity.Firestore.InitClient(ctx, s.config.ProjectId); err != nil {
		return err
	}
	if err = s.config.Entity.Pubsub.InitClient(ctx, s.config.ProjectId); err != nil {
		return err
	}
	return err
}

func (s *Service) initengine() {

	s.entityFactory = assembly.NewStreamEntityFactory(s.config.Entity)
	s.streamBuilder = engine.NewStreamBuilder(s.entityFactory)
}

func (s *Service) initRegistry(ctx context.Context) error {

	stream, err := s.streamBuilder.Build(ctx, s.config.Engine.RegSpec)
	if err != nil {
		return err
	}
	regExecutor := engine.NewExecutor(s.config.Engine, stream)
	registry := registry.NewStreamRegistry(s.config.Registry, regExecutor)

	if err := registry.Fetch(ctx); err != nil {
		return errors.New("error fetching registry data: " + err.Error())
	}
	s.registry = registry
	return nil
}

func (s *Service) initSupervisor(ctx context.Context) error {

	var err error

	s.supervisor, err = engine.NewSupervisor(ctx, s.config.Engine, s.streamBuilder, s.registry)
	if err != nil {
		return errors.New("error creating supervisor: " + err.Error())
	}

	if err = s.createAdminEventStream(ctx); err != nil {
		return err
	}

	if err := s.supervisor.Init(ctx); err != nil {
		return errors.New("error initializing supervisor: " + err.Error())
	}

	return nil
}

func (s *Service) createAdminEventStream(ctx context.Context) error {
	s.entityFactory.SetAdminLoader(s.supervisor.AdminEventHandler())
	stream, err := s.streamBuilder.Build(ctx, s.config.Engine.AdminSpec)
	if err != nil {
		return err
	}
	adminEventStreamExecutor := engine.NewExecutor(s.config.Engine, stream)
	s.supervisor.RegisterExecutor(ctx, adminEventStreamExecutor)
	s.registry.SetAdminStream(stream)
	return nil
}
