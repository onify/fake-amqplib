Changelog
=========

# 0.5.0

- support exclusive queue and its behaviour
- emit return on channel if mandatory message was not routed

# 0.4.0

- apparently connection is killed as well when trying to consume exclusive consumed queue
- try to mimic real behaviour and throw some errors with code

# 0.3.0

- kill channel if trying to consume exclusive consumed queue
