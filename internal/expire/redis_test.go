package expire

import (
	"strings"
	"testing"
)

// A malformed URL from a Secret must not put its password in the error (and
// from there in the pod log).
func TestRedisURLErrorHidesPassword(t *testing.T) {
	for _, u := range []string{"redis://:s3cret@cache:notaport/0", "redis://user:s3cret@cache:6379/nodb", "s3cret://%zz"} {
		_, err := NewRedisSink(u, "", "s", "f", 0)
		if err == nil {
			t.Fatalf("%s accepted", u)
		}
		if strings.Contains(err.Error(), "s3cret") {
			t.Errorf("error leaks the password: %v", err)
		}
	}
}
