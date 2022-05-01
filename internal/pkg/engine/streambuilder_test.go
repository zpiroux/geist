package engine

import (
	"fmt"
	"testing"
)

func Test_createInstanceAlias1(t *testing.T) {

	for i := 0; i < 3; i++ {
		fmt.Println(createInstanceAlias())
	}
}
