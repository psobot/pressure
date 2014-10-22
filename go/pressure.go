package pressure

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"os"
	"strconv"
)

type queueKeys struct {
	queue                 string
	bound                 string
	producer              string
	consumer              string
	producerFree          string
	consumerFree          string
	statsProducedMessages string
	statsProducedBytes    string
	statsConsumedMessages string
	statsConsumedBytes    string
	notFull               string
	closed                string
}

type PressureQueue struct {
	Pool       *redis.Pool
	Connection redis.Conn

	name      string
	clientUid string
	keys      queueKeys

	queueExists bool
	queueBound  int

	connected bool
	closed    bool
}

var BOUND_NOT_SET int = -1
var UNBOUNDED int = 0

var ErrPongFailure = errors.New("pressure: Redis did not return PONG")
var ErrCreationFailure = errors.New("Unexpected failure during queue creation.")
var ErrQueueAlreadyExists = errors.New("Queue already exists.")
var ErrQueueDoesNotExist = errors.New("Queue does not exist.")
var ErrQueueIsClosed = errors.New("Queue is closed.")

func key(prefix string, name string, key string) string {
	if key == "" {
		return fmt.Sprintf("%s:%s", prefix, name)
	} else {
		return fmt.Sprintf("%s:%s:%s", prefix, name, key)
	}
}

func NewPressureQueue(pool *redis.Pool, prefix string, name string) (*PressureQueue, error) {
	hostname, err := os.Hostname()

	if err != nil {
		return nil, err
	}

	c := pool.Get()

	pq := PressureQueue{
		Pool:       pool,
		Connection: c,
		name:       name,
		clientUid:  fmt.Sprintf("%s_pid%d", hostname, os.Getpid()),
		keys: queueKeys{
			queue:                 key(prefix, name, ""),
			bound:                 key(prefix, name, "bound"),
			producer:              key(prefix, name, "producer"),
			consumer:              key(prefix, name, "consumer"),
			producerFree:          key(prefix, name, "producer_free"),
			consumerFree:          key(prefix, name, "consumer_free"),
			statsProducedMessages: key(prefix, name, "stats_produced_messages"),
			statsProducedBytes:    key(prefix, name, "stats_produced_bytes"),
			statsConsumedMessages: key(prefix, name, "stats_consumed_messages"),
			statsConsumedBytes:    key(prefix, name, "stats_consumed_bytes"),
			notFull:               key(prefix, name, "not_full"),
			closed:                key(prefix, name, "closed"),
		},
		queueExists: false,
		queueBound:  BOUND_NOT_SET,
		connected:   false,
		closed:      false,
	}

	pong, err := redis.String(c.Do("PING"))
	if err != nil {
		return nil, err
	}
	if pong != "PONG" {
		return nil, ErrPongFailure
	}

	existing, err := redis.String(c.Do("GET", pq.keys.bound))
	if err != nil && err != redis.ErrNil {
		return nil, err
	}

	if existing != "" && err != redis.ErrNil {
		newBound, err := strconv.Atoi(existing)
		if err != nil {
			return nil, err
		}
		pq.queueBound = newBound
		pq.queueExists = true
	} else {
		pq.queueBound = BOUND_NOT_SET
		pq.queueExists = false
	}

	closed, err := redis.Bool(c.Do("EXISTS", pq.keys.closed))
	if err != nil {
		return nil, err
	}
	pq.closed = closed

	return &pq, nil
}

func (pq *PressureQueue) Create(bound int) error {
	return pq.create(pq.Connection, bound)
}

func (pq *PressureQueue) create(conn redis.Conn, bound int) error {
	//  Check if the queue already exists, or create it atomically.
	keyWasSet, err := redis.Bool(conn.Do("SETNX", pq.keys.bound, bound))

	if err != nil {
		return err
	}

	if keyWasSet {
		pq.queueExists = true
		pq.queueBound = bound

		var length int
		var err error

		length, err = redis.Int(conn.Do("LPUSH", pq.keys.producerFree, 0))
		if err != nil {
			return err
		}

		if length != 1 {
			return ErrCreationFailure
		}

		length, err = redis.Int(conn.Do("LPUSH", pq.keys.consumerFree, 0))
		if err != nil {
			return err
		}

		if length != 1 {
			return ErrCreationFailure
		}

		length, err = redis.Int(conn.Do("LPUSH", pq.keys.notFull, 0))
		if err != nil {
			return err
		}

		if length != 1 {
			return ErrCreationFailure
		}

		return nil
	} else {
		return ErrQueueAlreadyExists
	}
}

func (pq *PressureQueue) Put(buf []byte) error {
	return pq.put(pq.Connection, buf)
}

func (pq *PressureQueue) put(conn redis.Conn, buf []byte) (e error) {
	exists, err := redis.Bool(conn.Do("EXISTS", pq.keys.bound))

	if err != nil {
		return err
	}

	pq.queueExists = exists

	if !pq.queueExists {
		return ErrQueueDoesNotExist
	}

	_, err = conn.Do("BRPOP", pq.keys.producerFree, 0)
	if err != nil {
		return err
	}

	defer func() {
		_, err = conn.Do("LPUSH", pq.keys.producerFree, 0)
		if err != nil {
			e = err
		}
	}()

	_, err = conn.Do("SET", pq.keys.producer, pq.clientUid)
	if err != nil {
		return err
	}

	queueClosed, err := redis.Bool(conn.Do("EXISTS", pq.keys.closed))

	if queueClosed {
		return ErrQueueIsClosed
	} else {
		if pq.queueBound > 0 {
			_, err = conn.Do("BRPOP", pq.keys.notFull, 0)
		}

		queueLength, err := redis.Int(conn.Do("LPUSH", pq.keys.queue, buf))
		if err != nil {
			return err
		}

		if pq.queueBound != BOUND_NOT_SET && pq.queueBound != UNBOUNDED && queueLength < pq.queueBound {
			_, err = conn.Do("LPUSH", pq.keys.notFull, 0)
			if err != nil {
				return err
			}
			_, err = conn.Do("LTRIM", pq.keys.notFull, 0, 0)
			if err != nil {
				return err
			}
		}

		_, err = conn.Do("INCR", pq.keys.statsProducedMessages)
		if err != nil {
			return err
		}
		_, err = conn.Do("INCRBY", pq.keys.statsProducedBytes, len(buf))
		if err != nil {
			return err
		}
	}

	return nil
}

func (pq *PressureQueue) Get() (result []byte, e error) {
	return pq.get(pq.Connection)
}

func (pq *PressureQueue) get(conn redis.Conn) (result []byte, e error) {
	exists, err := redis.Bool(conn.Do("EXISTS", pq.keys.bound))

	if err != nil {
		return nil, err
	}

	pq.queueExists = exists

	if !pq.queueExists {
		return nil, ErrQueueDoesNotExist
	}

	_, err = conn.Do("BRPOP", pq.keys.consumerFree, 0)
	if err != nil {
		return nil, err
	}

	defer func() {
		_, err = conn.Do("LPUSH", pq.keys.consumerFree, 0)
		if err != nil {
			e = err
		}
	}()

	_, err = conn.Do("SET", pq.keys.consumer, pq.clientUid)
	if err != nil {
		return nil, err
	}

	isClosed, err := redis.Bool(conn.Do("EXISTS", pq.keys.closed))
	if err != nil {
		return nil, err
	}

	pq.closed = isClosed

	if pq.closed {
		queueEmpty, err := redis.Bool(conn.Do("EXISTS", pq.keys.queue))

		if queueEmpty {
			return nil, ErrQueueIsClosed
		} else {
			result, err = redis.Bytes(conn.Do("BRPOP", pq.keys.queue, 0))
			if err != nil {
				return nil, err
			} else {
				return result, nil
			}
		}
	} else {
		popped, err := redis.Values(conn.Do("BRPOP", pq.keys.queue, pq.keys.closed, 0))
		if err != nil {
			return nil, err
		}

		poppedKey, err := redis.String(popped[0], nil)
		if err != nil {
			return nil, err
		}

		if pq.keys.closed == poppedKey {
			pq.closed = true
			return nil, ErrQueueIsClosed
		} else {
			result, err := redis.Bytes(popped[1], nil)

			if err != nil {
				return nil, err
			}

			_, err = conn.Do("LPUSH", pq.keys.notFull, 0)
			if err != nil {
				return nil, err
			}
			_, err = conn.Do("LTRIM", pq.keys.notFull, 0, 0)
			if err != nil {
				return nil, err
			}

			_, err = conn.Do("INCR", pq.keys.statsConsumedMessages)
			if err != nil {
				return nil, err
			}
			_, err = conn.Do("INCRBY", pq.keys.statsConsumedBytes, len(result))
			if err != nil {
				return nil, err
			}

			return result, nil
		}
	}
}

func (pq *PressureQueue) Close() error {
	return pq.close(pq.Connection)
}

func (pq *PressureQueue) close(conn redis.Conn) (e error) {
	exists, err := redis.Bool(conn.Do("EXISTS", pq.keys.bound))

	if err != nil {
		return err
	}

	pq.queueExists = exists

	if !pq.queueExists {
		return ErrQueueDoesNotExist
	}

	_, err = conn.Do("BRPOP", pq.keys.producerFree, 0)
	if err != nil {
		return err
	}

	defer func() {
		_, err = conn.Do("LPUSH", pq.keys.producerFree, 0)
		if err != nil {
			e = err
		}
	}()

	_, err = conn.Do("SET", pq.keys.producer, pq.clientUid)
	if err != nil {
		return err
	}

	isClosed, err := redis.Bool(conn.Do("EXISTS", pq.keys.closed))
	if err != nil {
		return err
	}
	pq.closed = isClosed

	if pq.closed {
		return ErrQueueIsClosed
	} else {
		_, err = conn.Do("LPUSH", pq.keys.closed, 0, 0)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pq *PressureQueue) Delete() error {
	return pq.delete(pq.Connection)
}

func (pq *PressureQueue) delete(conn redis.Conn) (e error) {
	exists, err := redis.Bool(conn.Do("EXISTS", pq.keys.bound))

	if err != nil {
		return err
	}

	pq.queueExists = exists

	if !pq.queueExists {
		return ErrQueueDoesNotExist
	}

	_, err = conn.Do("DEL", pq.keys.bound)
	if err != nil {
		return err
	}

	_, err = conn.Do("LPUSH", pq.keys.notFull, 0)
	if err != nil {
		return err
	}

	_, err = conn.Do("LPUSH", pq.keys.closed, 0, 0)
	if err != nil {
		return err
	}

	_, err = conn.Do("BRPOP", pq.keys.producerFree, 0)
	if err != nil {
		return err
	}

	_, err = conn.Do("DEL", pq.keys.producer, pq.keys.producerFree)
	if err != nil {
		return err
	}

	_, err = conn.Do("BRPOP", pq.keys.consumerFree, 0)
	if err != nil {
		return err
	}

	_, err = conn.Do("DEL", pq.keys.consumer, pq.keys.consumerFree)
	if err != nil {
		return err
	}

	_, err = conn.Do("DEL",
		pq.keys.notFull,
		pq.keys.closed,
		pq.keys.statsProducedMessages,
		pq.keys.statsProducedBytes,
		pq.keys.statsConsumedMessages,
		pq.keys.statsConsumedBytes,
		pq.keys.queue)

	if err != nil {
		return err
	}

	pq.queueExists = false

	return nil
}

func (pq *PressureQueue) Exists() (bool, error) {
	return pq.exists(pq.Connection)
}

func (pq *PressureQueue) exists(conn redis.Conn) (bool, error) {
	exists, err := redis.Bool(conn.Do("EXISTS", pq.keys.bound))

	if err != nil {
		return false, err
	}

	pq.queueExists = exists
	return pq.queueExists, nil
}

func (pq *PressureQueue) Length() (int, error) {
	return pq.length(pq.Connection)
}

func (pq *PressureQueue) length(conn redis.Conn) (int, error) {
	length, err := redis.Int(conn.Do("LLEN", pq.keys.queue))

	if err == redis.ErrNil {
		exists, err := redis.Bool(conn.Do("EXISTS", pq.keys.bound))
		if err != nil {
			return 0, err
		}

		pq.queueExists = exists

		if pq.queueExists {
			return 0, nil
		} else {
			return 0, ErrQueueDoesNotExist
		}
	} else if err != nil {
		return 0, err
	} else {
		return length, nil
	}
}

func (pq *PressureQueue) IsClosed() (bool, error) {
	return pq.isClosed(pq.Connection)
}

func (pq *PressureQueue) isClosed(conn redis.Conn) (bool, error) {
	exists, err := pq.Exists()
	if err != nil {
		return true, err
	}
	if exists {
		isClosed, err := redis.Bool(conn.Do("EXISTS", pq.keys.closed))

		if err != nil {
			return true, err
		}

		pq.closed = isClosed
		return pq.closed, nil
	} else {
		return true, ErrQueueDoesNotExist
	}
}

func (pq *PressureQueue) getIntoChan(c chan []byte) {
	threadLocalConnection := pq.Pool.Get()
	defer threadLocalConnection.Close()

	for {
		data, err := pq.get(threadLocalConnection)
		if err == nil {
			c <- data
		} else if err == ErrQueueIsClosed {
			close(c)
			break
		} else {
			close(c)
			panic(err)
		}
	}
}

func (pq *PressureQueue) GetReadChan() <-chan []byte {
	return pq.GetBufferedReadChan(0)
}

func (pq *PressureQueue) GetBufferedReadChan(bufSize int) <-chan []byte {
	c := make(chan []byte, bufSize)
	go pq.getIntoChan(c)
	return c
}

func (pq *PressureQueue) putFromChan(c chan []byte) {
	threadLocalConnection := pq.Pool.Get()
	defer threadLocalConnection.Close()

	for elem := range c {
		err := pq.put(threadLocalConnection, elem)
		if err != nil {
			close(c)
			panic(err)
		}
	}
}

func (pq *PressureQueue) GetWriteChan() chan<- []byte {
	return pq.GetBufferedWriteChan(0)
}

func (pq *PressureQueue) GetBufferedWriteChan(bufSize int) chan<- []byte {
	c := make(chan []byte, bufSize)
	go pq.putFromChan(c)
	return c
}
