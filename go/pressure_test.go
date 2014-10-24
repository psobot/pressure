package pressure_test

import (
	"./"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func newPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func TestFunctions(t *testing.T) {
	pool := newPool(":6379", "")
	defer pool.Close()

	queue, err := pressure.NewPressureQueue(pool, "__pressure__", "__test__")
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

func TestChannels(t *testing.T) {
	pool := newPool(":6379", "")
	defer pool.Close()

	queue, err := pressure.NewPressureQueue(pool, "__pressure__", "__test__")
	require.Nil(t, err)

	queue.Delete()

	exists, err := queue.Exists()
	require.Nil(t, err)
	require.Equal(t, false, exists)

	err = queue.Create(100)
	require.Nil(t, err)

	write := queue.GetWriteChan()
	read := queue.GetReadChan()

	writeDone := make(chan bool)
	readDone := make(chan bool)

	go func() {
		write <- []byte("foo")
		write <- []byte("bar")
		write <- []byte("123")
		write <- []byte("456")
		writeDone <- true
	}()

	go func() {
		require.Equal(t, <-read, []byte("foo"))
		require.Equal(t, <-read, []byte("bar"))
		require.Equal(t, <-read, []byte("123"))
		require.Equal(t, <-read, []byte("456"))
		readDone <- true
	}()

	<-writeDone
	close(writeDone)

	<-readDone
	close(readDone)
}

func TestBufferedChannels(t *testing.T) {
	pool := newPool(":6379", "")
	defer pool.Close()

	queue, err := pressure.NewPressureQueue(pool, "__pressure__", "__test__")
	require.Nil(t, err)

	queue.Delete()

	exists, err := queue.Exists()
	require.Nil(t, err)
	require.Equal(t, false, exists)

	err = queue.Create(100)
	require.Nil(t, err)

	write := queue.GetBufferedWriteChan(5)
	read := queue.GetBufferedReadChan(5)

	writeDone := make(chan bool)
	readDone := make(chan bool)

	go func() {
		write <- []byte("foo")
		write <- []byte("bar")
		write <- []byte("123")
		write <- []byte("456")
		writeDone <- true
	}()

	go func() {
		require.Equal(t, <-read, []byte("foo"))
		require.Equal(t, <-read, []byte("bar"))
		require.Equal(t, <-read, []byte("123"))
		require.Equal(t, <-read, []byte("456"))
		readDone <- true
	}()

	<-writeDone
	close(writeDone)

	<-readDone
	close(readDone)
}
