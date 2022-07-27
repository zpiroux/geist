package assembly

import (
	"encoding/json"
	"fmt"

	"github.com/zpiroux/geist/entity"
)

type Config struct {
	Loaders    entity.LoaderFactories
	Extractors entity.ExtractorFactories
}

func (c Config) Close() error {

	var errs []string

	for _, lf := range c.Loaders {
		if err := lf.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	for _, ef := range c.Extractors {
		if err := ef.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}

	var err error
	if len(errs) > 0 {
		jerrs, _ := json.Marshal(errs)
		err = fmt.Errorf("error closing stream entities: %v", string(jerrs))
	}

	return err
}