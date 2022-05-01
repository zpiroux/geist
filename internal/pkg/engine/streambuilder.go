package engine

import (
	"context"
	"math/rand"
	"time"

	"github.com/zpiroux/geist/internal/pkg/igeist"
)

type StreamBuilder struct {
	entityFactory igeist.StreamEntityFactory
}

func NewStreamBuilder(entityFactory igeist.StreamEntityFactory) *StreamBuilder {
	return &StreamBuilder{entityFactory: entityFactory}
}

func (s *StreamBuilder) Build(ctx context.Context, spec igeist.Spec) (igeist.Stream, error) {

	instance := createInstanceAlias()

	extractor, err := s.entityFactory.CreateExtractor(ctx, spec, instance)
	if err != nil {
		return nil, err
	}
	transformer, err := s.entityFactory.CreateTransformer(ctx, spec)
	if err != nil {
		return nil, err
	}
	loader, err := s.entityFactory.CreateLoader(ctx, spec, instance)
	if err != nil {
		return nil, err
	}
	sinkExtractor, err := s.entityFactory.CreateSinkExtractor(ctx, spec, instance)
	if err != nil {
		return nil, err
	}

	return NewStream(spec, instance, extractor, transformer, loader, sinkExtractor), nil
}

// Since the actual/truly unique IDs of the stream instance, including Executor, Stream, and ETL entities
// are the struct pointers, which is what is used for execution logic, for the alias name we don't need
// to ensure 100% uniqueness. Thus, a shorter unique-enough alias is ok for simplified troubleshooting
// (and more readable than a uuid). With current combo of chars it's 1 chance in 5.5 million to have the
// same alias name, which is good enough for this purpose, and since stream instances are so few.
func createInstanceAlias() string {
	var a alias
	time.Sleep(10 * time.Millisecond) // this func is not performance critical, so ok to sleep a bit
	rand.Seed(time.Now().UnixNano())
	return a.cons().vow().cons().cons().vow().cons().name()
}

type alias struct {
	str string
}

func (a alias) vow() alias {
	var vowels = []rune{'a', 'e', 'i', 'o', 'u', 'y'}
	v := vowels[rand.Intn(len(vowels))]
	return alias{str: a.str + string(v)}
}

func (a alias) cons() alias {
	var consonants = []rune{'b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'm', 'n',
		'p', 'q', 'r', 's', 't', 'v', 'w', 'x', 'z'}
	c := consonants[rand.Intn(len(consonants))]
	return alias{str: a.str + string(c)}
}

func (a alias) name() string {
	return a.str
}
