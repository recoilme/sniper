package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPower(t *testing.T) {

	p, v := NextPowerOf2(256)
	if p != 8 || v != 256 {
		t.Errorf("get p = %d,v=%d want 8,256", p, v)
	}

	p, v = NextPowerOf2(1023)
	if p != 10 || v != 1024 {
		t.Errorf("get p = %d,v=%d want 10,1024", p, v)
	}

	p, v = NextPowerOf2(4294967294) //2^32-1-1
	if p != 32 || v != 4294967295 {
		t.Errorf("get p = %d,v=%d want 33,4294967295", p, v)
	}

	p, v = NextPowerOf2(3)
	if p != 2 || v != 4 {
		t.Errorf("get p = %d,v=%d want 2,4", p, v)
	}
	p, v = NextPowerOf2(0)
	if p != 0 || v != 0 {
		t.Errorf("get p = %d,v=%d want 0,0", p, v)
	}
}

func TestWrite(t *testing.T) {
	s, err := Open("1", 1)
	assert.NoError(t, err)
	s.Set([]byte("hello"), []byte("go"))
}
func TestHead(t *testing.T) {
	//b := [8]byte{0, 1, 0, 5, 0, 0, 0, 2}

}
