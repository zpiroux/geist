package service

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/internal/pkg/admin"
)

func TestInitServiceConfig(t *testing.T) {

	var c Config

	adminSpecInMem, err := entity.NewSpec(admin.AdminEventSpecInMem)
	assert.NoError(t, err)
	regSpecInMem, err := entity.NewSpec(admin.SpecRegistrationSpecInMem)
	assert.NoError(t, err)
	adminSpecNative, err := entity.NewSpec(admin.AdminEventSpec)
	assert.NoError(t, err)
	regSpecNative, err := entity.NewSpec(admin.SpecRegistrationSpec)
	assert.NoError(t, err)

	// Test correct default values (empty config)
	s := &Service{}
	err = s.initConfig(c)
	assert.NoError(t, err)
	assert.Equal(t, s.config.Registry.StorageMode, admin.RegStorageInMemory)
	assert.Equal(t, s.config.Registry.RegSpec, admin.SpecRegistrationSpecInMem)
	assert.Equal(t, s.config.Engine.RegSpec, regSpecInMem)
	assert.Equal(t, s.config.Engine.AdminSpec, adminSpecInMem)

	// Test correct values for native reg storage mode
	s = &Service{}
	c.Registry.StorageMode = admin.RegStorageNative
	err = s.initConfig(c)
	assert.NoError(t, err)
	assert.Equal(t, s.config.Registry.StorageMode, admin.RegStorageNative)
	assert.Equal(t, s.config.Registry.RegSpec, admin.SpecRegistrationSpec)
	assert.Equal(t, s.config.Engine.RegSpec, regSpecNative)
	assert.Equal(t, s.config.Engine.AdminSpec, adminSpecNative)

	// Test correct values for lib client provided (custom) Reg Spec
	s = &Service{}
	fileBytes, err := ioutil.ReadFile("../../test/specs/pubsubsrc-firestoresink-regspec.json")
	assert.NoError(t, err)
	c.Registry.StorageMode = admin.RegStorageCustom
	c.Registry.RegSpec = fileBytes
	err = s.initConfig(c)
	assert.NoError(t, err)
	assert.Equal(t, s.config.Registry.StorageMode, admin.RegStorageCustom)
	assert.Equal(t, s.config.Registry.RegSpec, fileBytes)
	regSpecCustomPubsub, err := entity.NewSpec(fileBytes)
	assert.NoError(t, err)
	assert.Equal(t, s.config.Engine.RegSpec, regSpecCustomPubsub)
	assert.Equal(t, s.config.Engine.AdminSpec, adminSpecNative)

	// TODO: Test correct values for lib client provided (custom) Admin Spec

}