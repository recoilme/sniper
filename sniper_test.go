package sniper

import (
	"bytes"
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

func TestCmd(t *testing.T) {
	err := DeleteStore("1")
	assert.NoError(t, err)

	s, err := Open("1")

	assert.NoError(t, err)
	err = s.Set([]byte("hello"), []byte("go"))
	assert.NoError(t, err)

	err = s.Set([]byte("hello"), []byte("world"))
	assert.NoError(t, err)
	res, err := s.Get([]byte("hello"))
	assert.NoError(t, err)
	assert.Equal(t, true, bytes.Equal(res, []byte("world")))
	assert.Equal(t, 1, s.Count())

	err = s.Close()
	s, err = Open("1")
	res, err = s.Get([]byte("hello"))
	assert.NoError(t, err)
	assert.Equal(t, true, bytes.Equal(res, []byte("world")))
	assert.Equal(t, 1, s.Count())

	deleted, err := s.Delete([]byte("hello"))
	assert.NoError(t, err)
	assert.True(t, deleted)
	assert.Equal(t, 0, s.Count())

	counter := []byte("counter")

	cnt, err := s.Incr(counter, uint64(1))
	assert.NoError(t, err)
	assert.Equal(t, 1, int(cnt))
	cnt, err = s.Incr(counter, uint64(42))
	assert.NoError(t, err)
	assert.Equal(t, 43, int(cnt))

	cnt, err = s.Decr(counter, uint64(2))
	assert.NoError(t, err)
	assert.Equal(t, 41, int(cnt))

	//overflow
	cnt, err = s.Decr(counter, uint64(42))
	assert.NoError(t, err)
	assert.Equal(t, uint64(18446744073709551615), uint64(cnt))

	err = s.Backup()
	assert.NoError(t, err)

	err = s.Close()
	assert.NoError(t, err)

	err = DeleteStore("1")
	assert.NoError(t, err)

}
