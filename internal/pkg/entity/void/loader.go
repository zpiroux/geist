package void

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/teltech/logger"
	"github.com/zpiroux/geist/entity"
)

var log *logger.Log

func init() {
	log = logger.New()
}

const sinkTypeId = "void"

type LoaderFactory struct {
}

func NewLoaderFactory() entity.LoaderFactory {
	return &LoaderFactory{}
}

func (lf *LoaderFactory) SinkId() string {
	return sinkTypeId
}

func (lf *LoaderFactory) NewLoader(ctx context.Context, spec *entity.Spec, id string) (entity.Loader, error) {
	return newLoader(spec)
}

func (lf *LoaderFactory) NewSinkExtractor(ctx context.Context, spec *entity.Spec, id string) (entity.Extractor, error) {
	return nil, nil
}

func (lf *LoaderFactory) Close() error {
	return nil
}

type loader struct {
	spec         *entity.Spec
	props        map[string]string
	maxErrors    int
	numberErrors int
}

func newLoader(spec *entity.Spec) (*loader, error) {
	l := &loader{
		spec:      spec,
		props:     make(map[string]string),
		maxErrors: math.MaxInt32,
	}

	if spec != nil {
		if spec.Sink.Config != nil {
			for _, prop := range spec.Sink.Config.Properties {
				l.props[prop.Key] = prop.Value
			}
			if value, ok := l.props["maxErrors"]; ok {
				l.maxErrors, _ = strconv.Atoi(value)
			}
		}
	}

	return l, nil
}

// inMemRegistryMode is used for internal test purposes
const inMemRegistryMode = "inMemRegistrySink"

func (l *loader) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {

	var (
		err        error
		retryable  bool
		resourceId = "<noResourceId>"
	)

	if l.spec.Ops.LogEventData {
		for _, transformed := range data {
			log.Infof("Received transformed event in void.loader: %s", transformed.String())
		}
	}

	resourceId, err, retryable = l.handleSinkMode(data, resourceId)
	if err != nil {
		return resourceId, err, retryable
	}

	err, retryable = l.handleSimulateError()

	if value, ok := l.props["logEventData"]; ok {
		if value == "true" {
			log.Infof("[voidsink] transformed.String() = %s", data[0].String())
		}
	}

	return resourceId, err, retryable
}

// handleSimulateError is used for e2e test of handling of retryable and unretryable sink errors.
// If used, the spec field maxErrors should also be set.
func (l *loader) handleSimulateError() (err error, retryable bool) {
	if errorType, ok := l.props["simulateError"]; ok {

		if l.numberErrors >= l.maxErrors {
			err = nil
			retryable = false
		} else {
			l.numberErrors++
			switch errorType {
			case "alwaysRetryable":
				err = errors.New("void loader simulating retryable error")
				retryable = true
			case "alwaysUnretryable":
				err = errors.New("void loader simulating unretryable error")
				retryable = false
			}
		}
	}
	return err, retryable
}

// handleSinkMode is used for e2e test of external registration of stream specs, providing
// properly returned resource/stream ID, without having a physical implementation of a spec store.
func (l *loader) handleSinkMode(data []*entity.Transformed, resourceId string) (string, error, bool) {
	if mode, ok := l.props["mode"]; ok {

		if mode == inMemRegistryMode {
			if data == nil {
				return resourceId, errors.New("streamLoad called without data to load (data == nil)"), false
			}
			if data[0] == nil {
				return resourceId, errors.New("streamLoad called without data to load (data[0] == nil)"), false
			}

			specData := data[0].Data["rawEvent"].(string)
			spec, err := entity.NewSpec([]byte(specData))
			if err != nil {
				return resourceId, fmt.Errorf("could not create Stream Spec from event data in in-mem Reg Sink StreamLoad, error: %s", err), false
			}
			resourceId = spec.Id()
		}
	}
	return resourceId, nil, false
}

func (l *loader) Shutdown() {
	// Nothing to shut down
}
