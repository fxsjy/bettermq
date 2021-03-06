# bettermq
A better message queue built by rust

I start this project to study Rust, and the idea is from Facebook's FOQS

protocol: https://github.com/fxsjy/bettermq/blob/main/proto/bettermq.proto

# bmq-cli 

```
    
USAGE:
    bmq-cli [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    ack        ack a message
    dequeue    get messages from queue
    enqueue    put a new message into queue
    help       Prints this message or the help of the given subcommand(s)

```
    
# bmq-cli enqueue 

```
put a new message into queue

USAGE:
    bmq-cli enqueue [OPTIONS]

FLAGS:
        --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -a, --after <DELIVERY AFTER>           [default: 0]
    -b, --benchmark <FOR MANY TIMES>       [default: 10]
    -f, --file <FILE NAME FOR PAYLOAD>    
    -h, --host <HOST ADDRESS>              [default: http://127.0.0.1:8404]
    -m, --meta <METAINFO>                  [default: meta]
    -p, --payload <MESSAGE DATA>           [default: ]
    -r, --priority <PRIORITY>              [default: 0]
    -t, --topic <TOPIC>                    [default: root]
```

# bmq-cli dequeue

```
get messages from queue

USAGE:
    bmq-cli dequeue [OPTIONS]

FLAGS:
        --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c, --count <COUNT>           [default: 1]
    -h, --host <HOST ADDRESS>     [default: http://127.0.0.1:8404]
    -l, --lease <LEASE>           [default: 0]
    -t, --topic <TOPIC>           [default: root]
```

# bmq-cli ack 

```
ack a message

USAGE:
    bmq-cli ack [OPTIONS] --id <MESSAGE ID>

FLAGS:
        --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -h, --host <HOST ADDRESS>     [default: http://127.0.0.1:8404]
    -i, --id <MESSAGE ID>        
    -t, --topic <TOPIC>           [default: root]
```
