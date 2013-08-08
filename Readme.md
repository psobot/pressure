# `pressure`
*A reimplementation of Python's synchronized and bounded Queue.Queue on a Redis backend.*

by Peter Sobot (psobot.com) on April 18, 2013. Licensed under MIT.

---

`pressure` implements everybody's favourite Python data structure, the
trusty built-in `Queue.Queue`, on top of everybody's favourite distributed
data store, Redis. Nearly all of the original Queue's API is replicated.
And amazingly, there are tests!

`pressure` allows for **synchronized**, **blocking** and **distributed**
(a.k.a.: multi-process) queues to be shared between processes, between 
machines, and even between data centers.

## Examples
    
    q1 = rqueue.Queue('test')
    q2 = rqueue.Queue('test')
    
    for string in ["hello", "goodbye"]:
        q1.put(string)

    result = q2.get(block=False)
    while result is not None:
        print result
        result = q2.get(block=False)

    # Prints:
    #   "hello"
    #   "goodbye"

## Get Started

    # make sure you have redis python bindings installed
    > sudo pip install redis

    # clone this repo
    > git clone git@github.com:psobot/pressure.git

    # for quick local testing, make sure you have redis-server running
    > redis-server &

    # muck around with ipython?
    > ipython

    Python 2.7.1 (r271:86832, Jun 16 2011, 16:59:05) 
    Type "copyright", "credits" or "license" for more information.

    IPython 0.13 -- An enhanced Interactive Python.
    ?         -> Introduction and overview of IPython's features.
    %quickref -> Quick reference.
    help      -> Python's own help system.
    object?   -> Details about 'object', use 'object??' for extra details.

    In [1]: import pressure
    In [2]: q1 = pressure.Queue('test_queue')
    In [3]: q1.put('hello springfield!')

## TODOs

    - Ensure that this library has 100% API coverage with the original Queue.Queue
    - Clean up and document the internals
    - Add some stuff to the readme about the reliable and buffered variants rqueue
    - Add more test cases
    - Formalize the ``protocol'' that this library uses, to allow other languages
      to also interface with this queue
    - Create a proof-of-concept version of this in C, maybe also Go?

## Questions/Comments/Feedback?

This *is* GitHub after all, so please feel free to open an issue if you discover
a bug. Better yet, feel free to fork this repo, add a test to cover the bug,
maybe even *fix* the bug, and send me back a pull request. You'll get a gold star
and a very happy tweet from me.

## LICENSE and all that jazz

    Copyright (c) 2013 Peter Sobot

    Permission is hereby granted, free of charge, to any person obtaining a
    copy of this software and associated documentation files (the "Software"),
    to deal in the Software without restriction, including without limitation
    the rights to use, copy, modify, merge, publish, distribute, sublicense,
    and/or sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
    THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
    DEALINGS IN THE SOFTWARE.

