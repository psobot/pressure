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
	Connection redis.Conn

	name      string
	clientUid string
	keys      queueKeys

	exists    bool
	connected bool
	closed    bool

	bound int
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

func NewPressureQueue(c redis.Conn, prefix string, name string) (*PressureQueue, error) {
	hostname, err := os.Hostname()

	if err != nil {
		return nil, err
	}

	pq := PressureQueue{
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
		exists:    false,
		connected: false,
		closed:    false,
		bound:     BOUND_NOT_SET,
	}

	pong, err := redis.String(pq.Connection.Do("PING"))
	if err != nil {
		return nil, err
	}
	if pong != "PONG" {
		return nil, ErrPongFailure
	}

	existing, err := redis.String(pq.Connection.Do("GET", pq.keys.bound))
	if err != nil && err != redis.ErrNil {
		return nil, err
	}

	if existing != "" && err != redis.ErrNil {
		newBound, err := strconv.Atoi(existing)
		if err != nil {
			return nil, err
		}
		pq.bound = newBound
		pq.exists = true
	} else {
		pq.bound = BOUND_NOT_SET
		pq.exists = false
	}

	closed, err := redis.Bool(c.Do("EXISTS", pq.keys.closed))
	if err != nil {
		return nil, err
	}
	pq.closed = closed

	return &pq, nil
}

func (pq *PressureQueue) Create(bound int) error {
	//  Check if the queue already exists, or create it atomically.
	keyWasSet, err := redis.Bool(pq.Connection.Do("SETNX", pq.keys.bound, bound))

	if err != nil {
		return err
	}

	if keyWasSet {
		pq.exists = true
		pq.bound = bound

		var length int
		var err error

		length, err = redis.Int(pq.Connection.Do("LPUSH", pq.keys.producerFree, 0))
		if err != nil {
			return err
		}

		if length != 1 {
			return ErrCreationFailure
		}

		length, err = redis.Int(pq.Connection.Do("LPUSH", pq.keys.consumerFree, 0))
		if err != nil {
			return err
		}

		if length != 1 {
			return ErrCreationFailure
		}

		length, err = redis.Int(pq.Connection.Do("LPUSH", pq.keys.notFull, 0))
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

func (pq *PressureQueue) Put(buf []byte) (e error) {
	exists, err := redis.Bool(pq.Connection.Do("EXISTS", pq.keys.bound))

	if err != nil {
		return err
	}

	pq.exists = exists

	if !pq.exists {
		return ErrQueueDoesNotExist
	}

	_, err = pq.Connection.Do("BRPOP", pq.keys.producerFree, 0)
	if err != nil {
		return err
	}

	defer func() {
		_, err = pq.Connection.Do("LPUSH", pq.keys.producerFree, 0)
		if err != nil {
			e = err
		}
	}()

	_, err = pq.Connection.Do("SET", pq.keys.producer, pq.clientUid)
	if err != nil {
		return err
	}

	queueClosed, err := redis.Bool(pq.Connection.Do("EXISTS", pq.keys.closed))

	if queueClosed {
		return ErrQueueIsClosed
	} else {
		if pq.bound > 0 {
			_, err = pq.Connection.Do("BRPOP", pq.keys.notFull, 0)
		}

		queueLength, err := redis.Int(pq.Connection.Do("LPUSH", pq.keys.queue, buf))
		if err != nil {
			return err
		}

		if pq.bound != BOUND_NOT_SET && pq.bound != UNBOUNDED && queueLength < pq.bound {
			_, err = pq.Connection.Do("LPUSH", pq.keys.notFull, 0)
			if err != nil {
				return err
			}
			_, err = pq.Connection.Do("LTRIM", pq.keys.notFull, 0, 0)
			if err != nil {
				return err
			}
		}

		_, err = pq.Connection.Do("INCR", pq.keys.statsProducedMessages)
		if err != nil {
			return err
		}
		_, err = pq.Connection.Do("INCRBY", pq.keys.statsProducedBytes, len(buf))
		if err != nil {
			return err
		}
	}

	return nil
}

func (pq *PressureQueue) Get() (result []byte, e error) {
	exists, err := redis.Bool(pq.Connection.Do("EXISTS", pq.keys.bound))

	if err != nil {
		return nil, err
	}

	pq.exists = exists

	if !pq.exists {
		return nil, ErrQueueDoesNotExist
	}

	_, err = pq.Connection.Do("BRPOP", pq.keys.consumerFree, 0)
	if err != nil {
		return nil, err
	}

	defer func() {
		_, err = pq.Connection.Do("LPUSH", pq.keys.consumerFree, 0)
		if err != nil {
			e = err
		}
	}()

	_, err = pq.Connection.Do("SET", pq.keys.consumer, pq.clientUid)
	if err != nil {
		return nil, err
	}

	isClosed, err := redis.Bool(pq.Connection.Do("EXISTS", pq.keys.closed))
	if err != nil {
		return nil, err
	}

	pq.closed = isClosed

	if pq.closed {
		queueEmpty, err := redis.Bool(pq.Connection.Do("EXISTS", pq.keys.queue))

		if queueEmpty {
			return nil, ErrQueueIsClosed
		} else {
			result, err = redis.Bytes(pq.Connection.Do("BRPOP", pq.keys.queue, 0))
			if err != nil {
				return nil, err
			} else {
				return result, nil
			}
		}
	} else {
		popped, err := redis.Values(pq.Connection.Do("BRPOP", pq.keys.queue, pq.keys.closed, 0))
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

			_, err = pq.Connection.Do("LPUSH", pq.keys.notFull, 0)
			if err != nil {
				return nil, err
			}
			_, err = pq.Connection.Do("LTRIM", pq.keys.notFull, 0, 0)
			if err != nil {
				return nil, err
			}

			_, err = pq.Connection.Do("INCR", pq.keys.statsConsumedMessages)
			if err != nil {
				return nil, err
			}
			_, err = pq.Connection.Do("INCRBY", pq.keys.statsConsumedBytes, len(result))
			if err != nil {
				return nil, err
			}

			return result, nil
		}
	}
}

func (pq *PressureQueue) Close() (e error) {
	exists, err := redis.Bool(pq.Connection.Do("EXISTS", pq.keys.bound))

	if err != nil {
		return err
	}

	pq.exists = exists

	if !pq.exists {
		return ErrQueueDoesNotExist
	}

	_, err = pq.Connection.Do("BRPOP", pq.keys.producerFree, 0)
	if err != nil {
		return err
	}

	defer func() {
		_, err = pq.Connection.Do("LPUSH", pq.keys.producerFree, 0)
		if err != nil {
			e = err
		}
	}()

	_, err = pq.Connection.Do("SET", pq.keys.producer, pq.clientUid)
	if err != nil {
		return err
	}

	isClosed, err := redis.Bool(pq.Connection.Do("EXISTS", pq.keys.closed))
	if err != nil {
		return err
	}
	pq.closed = isClosed

	if pq.closed {
		return ErrQueueIsClosed
	} else {
		_, err = pq.Connection.Do("LPUSH", pq.keys.closed, 0, 0)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pq *PressureQueue) Delete() (e error) {
	exists, err := redis.Bool(pq.Connection.Do("EXISTS", pq.keys.bound))

	if err != nil {
		return err
	}

	pq.exists = exists

	if !pq.exists {
		return ErrQueueDoesNotExist
	}

	_, err = pq.Connection.Do("DEL", pq.keys.bound)
	if err != nil {
		return err
	}

	_, err = pq.Connection.Do("LPUSH", pq.keys.notFull, 0)
	if err != nil {
		return err
	}

	_, err = pq.Connection.Do("LPUSH", pq.keys.closed, 0, 0)
	if err != nil {
		return err
	}

	_, err = pq.Connection.Do("BRPOP", pq.keys.producerFree, 0)
	if err != nil {
		return err
	}

	_, err = pq.Connection.Do("DEL", pq.keys.producer, pq.keys.producerFree)
	if err != nil {
		return err
	}

	_, err = pq.Connection.Do("BRPOP", pq.keys.consumerFree, 0)
	if err != nil {
		return err
	}

	_, err = pq.Connection.Do("DEL", pq.keys.consumer, pq.keys.consumerFree)
	if err != nil {
		return err
	}

	_, err = pq.Connection.Do("DEL",
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

	pq.exists = false

	return nil
}

func (pq *PressureQueue) Exists() (bool, error) {
	exists, err := redis.Bool(pq.Connection.Do("EXISTS", pq.keys.bound))

	if err != nil {
		return false, err
	}

	pq.exists = exists
	return pq.exists, nil
}

func (pq *PressureQueue) Length() (int, error) {
	length, err := redis.Int(pq.Connection.Do("LLEN", pq.keys.queue))

	if err == redis.ErrNil {
		exists, err := redis.Bool(pq.Connection.Do("EXISTS", pq.keys.bound))
		if err != nil {
			return 0, err
		}

		pq.exists = exists

		if pq.exists {
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
	exists, err := pq.Exists()
	if err != nil {
		return true, err
	}
	if exists {
		isClosed, err := redis.Bool(pq.Connection.Do("EXISTS", pq.keys.closed))

		if err != nil {
			return true, err
		}

		pq.closed = isClosed
		return pq.closed, nil
	} else {
		return true, ErrQueueDoesNotExist
	}
}
