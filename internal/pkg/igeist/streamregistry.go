package igeist

// A StreamRegistry is a Registry also managing a stream as an Executor, receiving Specs to be
// added to registry in an arbitrary ETL stream, e.g. via Kafka or REST API into a db.
// It is also required to publish admin events when registry is modified (e.g. new stream registrations, etc).
type StreamRegistry interface {
	Registry
	Executor
	SetAdminStream(stream Stream)
}
