package igeist

type Spec interface {
	Id() string       // Returns Stream Spec ID
	IsDisabled() bool // True if spec is disabled and should not be run by Executor
	Validate() error  // Returns an error if Spec is not according to spec
	JSON() []byte     // Returns spec data as JSON
}
