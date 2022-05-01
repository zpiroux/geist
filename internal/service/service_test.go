package service

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/internal/pkg/model"
)

const (
	apiKey    = "myCoolApiKey"
	apiSecret = "myCoolApiSecret"
)

func TestServiceConfigLogging(t *testing.T) {

	var s Service

	s.config.Entity.Kafka.ConfluentApiKey = apiKey
	s.config.Entity.Kafka.ConfluentApiSecret = apiSecret

	status := s.String()
	assert.True(t, strings.Contains(status, apiKey))
	assert.False(t, strings.Contains(status, apiSecret))
}

func TestInitServiceConfig(t *testing.T) {

	var c Config

	adminSpecInMem, err := model.NewSpec(model.AdminEventSpecInMem)
	assert.NoError(t, err)
	regSpecInMem, err := model.NewSpec(model.SpecRegistrationSpecInMem)
	assert.NoError(t, err)
	adminSpecNative, err := model.NewSpec(model.AdminEventSpec)
	assert.NoError(t, err)
	regSpecNative, err := model.NewSpec(model.SpecRegistrationSpec)
	assert.NoError(t, err)

	// Test correct default values (empty config)
	s := &Service{}
	err = s.initConfig(c)
	assert.NoError(t, err)
	assert.Equal(t, s.config.Registry.StorageMode, model.RegStorageInMemory)
	assert.Equal(t, s.config.Registry.RegSpec, model.SpecRegistrationSpecInMem)
	assert.Equal(t, s.config.Engine.RegSpec, regSpecInMem)
	assert.Equal(t, s.config.Engine.AdminSpec, adminSpecInMem)

	// Test correct values for native reg storage mode
	s = &Service{}
	c.Registry.StorageMode = model.RegStorageNative
	err = s.initConfig(c)
	assert.NoError(t, err)
	assert.Equal(t, s.config.Registry.StorageMode, model.RegStorageNative)
	assert.Equal(t, s.config.Registry.RegSpec, model.SpecRegistrationSpec)
	assert.Equal(t, s.config.Engine.RegSpec, regSpecNative)
	assert.Equal(t, s.config.Engine.AdminSpec, adminSpecNative)

	// Test correct values for lib client provided (custom) Reg Spec
	s = &Service{}
	fileBytes, err := ioutil.ReadFile("../../test/specs/pubsubsrc-firestoresink-regspec.json")
	assert.NoError(t, err)
	c.Registry.StorageMode = model.RegStorageCustom
	c.Registry.RegSpec = fileBytes
	err = s.initConfig(c)
	assert.NoError(t, err)
	assert.Equal(t, s.config.Registry.StorageMode, model.RegStorageCustom)
	assert.Equal(t, s.config.Registry.RegSpec, fileBytes)
	regSpecCustomPubsub, err := model.NewSpec(fileBytes)
	assert.NoError(t, err)
	assert.Equal(t, s.config.Engine.RegSpec, regSpecCustomPubsub)
	assert.Equal(t, s.config.Engine.AdminSpec, adminSpecNative)

	// TODO: Test correct values for lib client provided (custom) Admin Spec

}
