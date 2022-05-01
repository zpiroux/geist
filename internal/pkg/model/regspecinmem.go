package model

// SpecRegistrationSpecInMem is the in-mem alternative to SpecRegistrationSpec (see that one for more detailed doc)
var SpecRegistrationSpecInMem = []byte(`
{
   "namespace": "geist",
   "streamIdSuffix": "specs",
   "description": "A stream ingestion of GEIST Specs, with source set as GEIST API, no Transformation, and no sink (specs stored in-memory).",
   "version": 1,
   "source": {
      "type": "geistapi"
   },
   "transform": {
      "extractFields": [
         {
            "fields": [
               {
                  "id": "rawEvent",
                  "type": "string"
               }
            ]
         }
      ]
   },
   "sink": {
      "type": "void",
      "config": {
         "properties": [
            {
               "key": "mode",
               "value": "inMemRegistrySink"
            }
         ]
      }
   }
}`)
