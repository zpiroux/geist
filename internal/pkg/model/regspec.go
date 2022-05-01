package model

// SpecRegistrationSpec is the internal, built-in GEIST spec required for handling of
// GEIST specs themselves. It defines how specs can be received from the chosen Source,
// and where and how to store them. This spec cannot store itself into that repository for
// bootstrap reasons.
// We can have a number of these supported spec registration flows here, and it's up to the
// GEIST app service init to choose the appropriate one.
// We can easily change the spec reg flow to have GEIST spec deployments be done by sending specs
// to pubsub instead. Just switch spec.source.type to "pubsub" and add pubsub topic config.
var SpecRegistrationSpec = []byte(`
{
   "namespace": "geist",
   "streamIdSuffix": "specs",
   "description": "A stream ingestion of GEIST Specs, with source set as GEIST REST API, no Transformation, and into a Firestore Sink (GEIST Registry).",
   "version": 1,
   "source": {
      "type": "geistapi"
   },
   "transform": {
      "extractFields": [
         {
            "fields": [
               {
                  "id": "namespace",
                  "jsonPath": "namespace"
               },
               {
                  "id": "idSuffix",
                  "jsonPath": "streamIdSuffix"
               },
               {
                  "id": "description",
                  "jsonPath": "description"
               },
               {
                  "id": "version",
                  "jsonPath": "version",
                  "type": "integer"
               },
               {
                  "id": "disabled",
                  "jsonPath": "disabled",
                  "type": "bool"
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
      "type": "firestore",
      "config": {
         "kinds": [
            {
               "name": "EtlSpec",
               "entityNameFromIds": {
                  "ids": [
                     "namespace",
                     "idSuffix"
                  ],
                  "delimiter": "-"
               },
               "properties": [
                  {
                     "id": "version",
                     "name": "version",
                     "index": true
                  },
                  {
                     "id": "description",
                     "name": "description",
                     "index": false
                  },
                  {
                     "id": "disabled",
                     "name": "disabled",
                     "index": true
                  },
                  {
                     "id": "rawEvent",
                     "name": "specData",
                     "index": false
                  }
               ]
            }
         ]
      }
   }
}`)
