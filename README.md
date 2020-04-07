# README

Mock IQFeed server that can turn historical data into live data.

## Development

### Building

    docker-compose build

### Validate Syntax

    docker run iqfeedserver-tests syntax

### Running

    docker run -p 9999:9999 okinta/iqfeedserver

### Testing

Make sure iqfeedserver is running. Then run:

    docker run iqfeedserver-tests
