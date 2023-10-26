Changelog
=========

# 3.0.0

- acking/nacking/rejecting an already acked message kills the channel with an unknown delivery tag error (406)
- support per channel prefetch, kills connection if prefetch is called with global argument before version 3.3
- nackAll requeue argument is true by default, as per documentation
- fake connection and channel inherits from EventEmitter

# 2.0.0

- drop node 12 support
- convert to esm with exports for node
- bump `smqp@8`
- lint some

# 1.0.0

- smqp@6.1
- lint some

# 0.9.1

- Always return queue name when asserting queue

# 0.9.0

- support connecting with urlish object
- smqp@6

# 0.8.5

- ack/nack all only cares about messages consumed by channel, previously everything was gone

# 0.8.4

- ack/nack all fix

# 0.8.3

- Call confirm channel callback when the message is queued, not when it is consumed!
- implement publish with empty string special case
- hide some internal props from message

# 0.8.2

- share behind the scenes broker if connection hosts and vhost are the same
- add new `connectSync` helper method to be able to get a connection synchronously to facilitate testing

# 0.8.1

- be a better mimic of amqplib, some stuff didn't work at all prior to this version

## Additions

- Handle different behaviours between RabbitMQ versions

# 0.8.0

- bump `smqp@5`
- stop building for node 10 (mocha's fault)

# 0.7.0

- bump `smqp@4`

# 0.6.0

- bump `smqp@3.2`

# 0.5.0

- support exclusive queue and its behaviour
- emit return on channel if mandatory message was not routed

# 0.4.0

- apparently connection is killed as well when trying to consume exclusive consumed queue
- try to mimic real behaviour and throw some errors with code

# 0.3.0

- kill channel if trying to consume exclusive consumed queue
