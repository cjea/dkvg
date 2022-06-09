<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [dkvg](#dkvg)
  - [Description](#description)
  - [Run](#run)
    - [Multiple clients](#multiple-clients)
    - [REPL (local development)](#repl-local-development)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# dkvg

## Description
distributed key value in Go

## Run
### Multiple clients
```bash
$ make run

# in separate tab(s)
$ make client
#> set foo = bar
OK
#> get foo
bar
```

### REPL (local development)
Running with `--repl` enabled has the server read from STDIN instead of a UNIX socket, so it can only accept input from a single client.
```bash
$ go run main.go --repl
#> set foo=bar
OK
#> quit
Goodbye
```
