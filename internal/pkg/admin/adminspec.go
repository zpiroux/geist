package admin

const (
	EventNameKey    = "eventName"
	EventRawDataKey = "rawEvent"
)

var AdminEventSpec = []byte(`
{
   "namespace": "geist",
   "streamIdSuffix": "adminevents",
   "description": "A built-in GEIST-internal admin notification stream. For example, when a new or updated spec is written to Registry db, Registry sends an event to pubsub, which are listened to by all GEIST deployments. This in turn makes Registries, not yet synchronized, load the new specs from db.",
   "version": 1,
   "source": {
      "type": "pubsub",
      "config": {
         "topics": [
            {
               "env": "all",
               "names": [
                  "geist-admin-events"
               ]
            }
         ],
         "subscription": {
            "type": "unique"
         }
      }
   },
   "transform": {
      "extractFields": [
         {
            "fields": [
               {
                  "id": "eventName",
                  "jsonPath": "name"
               },
               {
                  "id": "rawEvent",
                  "type": "string"
               }
            ]
         }
      ]
   },
   "sink": {
      "type": "admin"
   }
}`)
