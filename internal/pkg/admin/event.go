package admin

import (
	"time"

	"github.com/google/uuid"
)

// Admin event and operation types.
const (
	EventStreamRegistryModified = "stream_registry_modified"
)

const (
	OperationStreamRegistration = "streamRegistration"
)

const adminEventVersion = "0.0.1"

type AdminEvent struct {
	Name         string           `json:"name"`
	DateOccurred time.Time        `json:"dateOccurred"`
	Version      string           `json:"version"`
	EventId      string           `json:"eventId"`
	Location     Location         `json:"location"`
	Data         []AdminEventData `json:"data"`
}

type Location struct {
	Service string `json:"service"`
}

type AdminEventData struct {
	Operation string `json:"operation,omitempty"`
	StreamId  string `json:"streamId,omitempty"`
}

func NewAdminEvent(name string, operation string, streamId string) AdminEvent {

	data := AdminEventData{
		Operation: operation,
		StreamId:  streamId,
	}
	return AdminEvent{
		Name:         name,
		DateOccurred: time.Now(),
		Version:      adminEventVersion,
		EventId:      uuid.New().String(),
		Location:     Location{Service: "geist"},
		Data:         []AdminEventData{data},
	}
}

/* Example:

{
   "name": "stream_registry_modified",
   "dateOccurred": "2019-11-30T14:57:23.389Z",
   "version": "1.0",
   "eventId": "d9d11ff8-b5b5-4e40-91fe-b45301b9e96f",
   "data": [
      {
         "operation": "streamRegistration",
         "streamId": "geisttest-imbatestspec"
      }
   ]
}

*/
