package etltest

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/internal/pkg/admin"
	"github.com/zpiroux/geist/internal/pkg/igeist"
)

const (
	SpecApiSrcBigtableSinkMinimal        = "ApiSrcBigtableSinkMinimal"
	SpecApiSrcBigtableSinkFooRound       = "ApiSrcBigtableSinkFooRound"
	SpecKafkaSrcBigtableSinkPlayer       = "KafkaSrcBigtableSinkPlayer"
	SpecKafkaSrcBigtableSinkMultiSession = "KafkaSrcBigtableSinkMultiSession"
	SpecKafkaSrcBigtableSinkFeatureX     = "KafkaSrcBigtableSinkFeatureX"
	SpecRegSpecPubsub                    = "PubsubSrcFirestoreSinkRegSpec"
	SpecPubsubSrcKafkaSinkFoologs        = "PubsubSrcKafkaSinkFoologs"
)

var tstreamSpecs = map[string]string{
	SpecApiSrcBigtableSinkMinimal:        "specs/apisrc-bigtablesink-minimal.json",
	SpecApiSrcBigtableSinkFooRound:       "specs/apisrc-bigtablesink-fooround.json",
	SpecKafkaSrcBigtableSinkPlayer:       "specs/kafkasrc-bigtablesink-user.json",
	SpecKafkaSrcBigtableSinkMultiSession: "specs/kafkasrc-bigtablesink-multitable-session.json",
	SpecKafkaSrcBigtableSinkFeatureX:     "specs/kafkasrc-bigtablesink-featurex.json",
	SpecRegSpecPubsub:                    "specs/pubsubsrc-firestoresink-regspec.json",
	SpecPubsubSrcKafkaSinkFoologs:        "specs/pubsubsrc-kafkasink-foologs.json",
}

// Convenience test function to get Spec for handling GEIST specs without loading Registry
func SpecSpec() *entity.Spec {
	spec, _ := entity.NewSpec(admin.SpecRegistrationSpec)
	return spec
}

func SpecSpecInMem() *entity.Spec {
	spec, _ := entity.NewSpec(admin.SpecRegistrationSpecInMem)
	return spec
}

// Maybe not needed (although useful for test purposes)
func GetAllSpecsRaw(testDirPath string) map[string][]byte {
	specs := make(map[string][]byte)
	for key, filePath := range tstreamSpecs {
		fileBytes, err := os.ReadFile(testDirPath + filePath)
		if err != nil || fileBytes == nil {
			panic("couldn't read all test spec files, err: " + err.Error())
		}
		specs[key] = fileBytes
	}
	return specs
}

// In-memory SpecRegistry implementation for GEIST specs for testing purposes
// For real usage this is replaced by a proper cloud db backed registry
// implementation like a cloud firestore one, with dynamic updates of specs.
type StreamRegistry struct {
	testDirPath string
	executor    igeist.Executor
	specs       map[string]igeist.Spec
}

func NewStreamRegistry(testDirPath string) *StreamRegistry {
	var r StreamRegistry
	r.specs = make(map[string]igeist.Spec)
	r.testDirPath = testDirPath

	streamBuilder := NewStreamBuilder(&StreamEntityFactory{})
	stream, err := streamBuilder.Build(context.Background(), SpecSpec())
	if err != nil || stream == nil {
		return nil
	}
	r.executor = NewExecutor(stream)
	return &r
}

func (r *StreamRegistry) Put(ctx context.Context, id string, spec igeist.Spec) error {
	if err := spec.(*entity.Spec).Validate(); err != nil {
		return err
	}
	r.specs[id] = spec.(*entity.Spec)
	return nil
}

func (r *StreamRegistry) Fetch(ctx context.Context) error {

	for specId, file := range tstreamSpecs {
		err := r.registerSpec(ctx, specId, file)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *StreamRegistry) Get(ctx context.Context, id string) (igeist.Spec, error) {
	return r.specs[id], nil
}

func (r *StreamRegistry) GetAll(ctx context.Context) (map[string]igeist.Spec, error) {
	return r.specs, nil
}

func (r *StreamRegistry) Delete(ctx context.Context, id string) error {
	return nil
}

func (r *StreamRegistry) Exists(id string) bool {
	return false
}

func (r *StreamRegistry) ExistsWithSameOrHigherVersion(specBytes []byte) (bool, error) {
	return false, nil
}

func (r *StreamRegistry) Validate(specBytes []byte) (igeist.Spec, error) {
	spec, err := entity.NewSpec(specBytes)
	if err == nil && spec != nil {
		err = spec.Validate()
	} else {
		err = fmt.Errorf("could not create spec from spec data, err: %v, spec data: %s", err, string(specBytes))
	}
	return spec, err
}

func (r *StreamRegistry) registerSpec(ctx context.Context, id string, path string) error {

	path = r.testDirPath + path
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	spec, err := entity.NewSpec(fileBytes)
	if err != nil {
		return err
	}
	return r.Put(ctx, id, spec)
}

func (r *StreamRegistry) SetAdminStream(stream igeist.Stream) {
	// Admin stream not part of these tests
}

// Executor interface impl

func (r *StreamRegistry) StreamId() string {
	return r.executor.StreamId()
}

func (r *StreamRegistry) Spec() igeist.Spec {
	return SpecSpec()
}

func (r *StreamRegistry) Metrics() entity.Metrics {
	return r.executor.Metrics()
}

func (r *StreamRegistry) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
}

func (r *StreamRegistry) ProcessEvent(ctx context.Context, events []entity.Event) entity.EventProcessingResult {
	return entity.EventProcessingResult{}
}

func (r *StreamRegistry) Stream() igeist.Stream {
	return r.executor.Stream()
}

func (r *StreamRegistry) Shutdown(ctx context.Context) {
	// Nothing to mock here
}
