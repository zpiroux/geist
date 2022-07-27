/*
Package transform is the native/default implementation of a transform provider.
It is made externally accessible since it's useful for developing/testing extractor/loader plug-ins.

It is currently not yet possible to provide custom transform providers/plug-ins, in contrast to source/sink ones.
Client-provided custom transformation logic can instead be added via the pre-transform hook function.
*/
package transform
