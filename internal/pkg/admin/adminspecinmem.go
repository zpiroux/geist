package admin

var AdminEventSpecInMem = []byte(`
{
   "namespace": "geist",
   "streamIdSuffix": "adminevents-inmem",
   "description": "When running Registry in-mem, there's no point in having cross-pod admin events, so this spec is a bare-bones naked one'",
   "version": 1,
   "source": {
      "type": "geistapi"
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
