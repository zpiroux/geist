package admin

const (
	EventNameKey    = "eventName"
	EventRawDataKey = "rawEvent"
)

// AdminEventSpec is the native admin spec that will be used if the config parameter
// geist.Config.SpecRegistryConfig.StorageMode is set to "native". If instead set to
// "custom" any custom type of stream spec can be provided to be used as the admin stream.
// If omitted in the config, a default in-memory admin stream will be used.
//
// The admin stream is used as a cross-pod synchronization mechanism. For example, when
// a new or updated spec is written to Registry db, Registry sends an event to admin stream's
// sorce entity, which are listened to by all GEIST deployments. This in turn makes Registries,
// not yet synchronized, load the new specs from the db.
//
// TODO: Remove this legacy native admin spec, and provide it as an example spec instead.
var AdminEventSpec = []byte(`
{
   "namespace": "geist",
   "streamIdSuffix": "adminevents",
   "description": "A built-in GEIST-internal admin notification stream",
   "version": 1,
   "source": {
      "type": "pubsub",
      "config": {
         "customConfig": {
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
}
`)
