Kafka protocol uses streaming for both - requests and responses. Client sends requests
one after another without waiting for a response, but setting a unique number - 
correlation id - to each request. Kafka broker processes requests and sends responses
with the same correlation id in header. Client uses this correlation id to map responses
to requests.

This protocol dictates implementation design. In Kafka.jl client and request lifecycle
look as follow:

Client initialization:

1. Client connects to a bootstrap broker, reads metadata about the whole cluster
   and creates a socket connection to each available broker.
2. For each socket, client runs a background task waiting for data from the socket.
3. Client also creates an output channel per socket to put read messages to.

Request lifecycle:

1. In main task, client sends a request with a unique correlation id and stores
   mapping `correlation id -> request type`.
2. Client immediately starts to wait for a response in the corresponding output channel.     
3. Kafka broker processes the request and sends the response.
4. Background task reads the header to determine correlation id and finds corresponding
   request type (saved on step (1)). It then reads appropriate response from the socket
   and puts it into corresponding output channel. 
5. The main task takes the response from the channel and coninues processing.