package void

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/teltech/logger"
	"github.com/zpiroux/geist/internal/pkg/model"
)

var log *logger.Log

func init() {
	log = logger.New()
}

const inMemRegistryMode = "inMemRegistrySink"

type loader struct {
	spec         *model.Spec
	props        map[string]string
	maxErrors    int
	numberErrors int
}

func NewLoader(spec *model.Spec) (*loader, error) {
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

func (l *loader) StreamLoad(ctx context.Context, data []*model.Transformed) (string, error, bool) {

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

	if value, ok := l.props["simulateError"]; ok {

		if l.numberErrors >= l.maxErrors {
			err = nil
			retryable = false
		} else {
			l.numberErrors++
			switch value {
			case "alwaysRetryable":
				err = errors.New("void loader simulating retryable error")
				retryable = true
			case "alwaysUnretryable":
				err = errors.New("void loader simulating unretryable error")
				retryable = false
			}
		}
	}

	if value, ok := l.props["mode"]; ok {
		if value == inMemRegistryMode {
			if data == nil {
				return resourceId, errors.New("streamLoad called without data to load (data == nil)"), false
			}
			if data[0] == nil {
				return resourceId, errors.New("streamLoad called without data to load (data[0] == nil)"), false
			}

			specData := data[0].Data["rawEvent"].(string)
			spec, err := model.NewSpec([]byte(specData))
			if err != nil {
				return resourceId, fmt.Errorf("could not create Stream Spec from event data in in-mem Reg Sink StreamLoad, error: %s", err), false
			}
			resourceId = spec.Id()
		}
	}

	if value, ok := l.props["logEventData"]; ok {
		if value == "true" {
			log.Infof("event received in Void sink StreamLoad: %s", data[0].String())
		}
	}

	return resourceId, err, retryable
}

func (l *loader) Shutdown() {}
