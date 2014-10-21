package pressure_test

import (
	"./"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAll(t *testing.T) {
	c, err := redis.Dial("tcp", ":6379")
	require.Nil(t, err)
	defer c.Close()

	queue, err := pressure.NewPressureQueue(c, "__pressure__", "__test__")
	require.Nil(t, err)

	queue.Delete()

	exists, err := queue.Exists()
	require.Nil(t, err)
	require.Equal(t, false, exists)

	err = queue.Create(100)
	require.Nil(t, err)

	exists, err = queue.Exists()
	require.Nil(t, err)
	require.Equal(t, true, exists)

	err = queue.Put([]byte("test"))
	require.Nil(t, err)

	length, err := queue.Length()
	require.Nil(t, err)
	require.Equal(t, 1, length)

	bytes, err := queue.Get()
	require.Nil(t, err)

	require.Equal(t, []byte("test"), bytes)

	err = queue.Close()
	require.Nil(t, err)

	isClosed, err := queue.IsClosed()
	require.Nil(t, err)
	require.Equal(t, true, isClosed)

	err = queue.Delete()
	require.Nil(t, err)

	exists, err = queue.Exists()
	require.Nil(t, err)
	require.Equal(t, false, exists)
}
