package admin

//
// Extractor/Sink Channel for chan based etl entities, for example as used for GEIST API sinks, where
// the external service publish incoming events on the Extractor's source channel.
//

type ResultChanEvent struct {
	Id      string // Id of published event (created resource), according to rules in ETL stream spec
	Success bool   // Result status that will only be true if set explicitly, avoiding default value issues with Err
	Error   error  // Contains error info in case of Success == false
}

type ChanEvent struct {
	Event         any // TODO: change to []byte
	ResultChannel chan ResultChanEvent
}

type EventChannel chan ChanEvent
type ResultChannel <-chan ResultChanEvent
