package model

type RegStorageMode string

const (
	RegStorageUndefined RegStorageMode = ""
	RegStorageNative    RegStorageMode = "native"
	RegStorageInMemory  RegStorageMode = "inmemory"
	RegStorageCustom    RegStorageMode = "custom"
)
