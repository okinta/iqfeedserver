# README

Mock IQFeed server that can turn historical data into live data.

## Development

### Building

    docker build -t okinta/iqfeedserver -f containers/iqfeedserver/Dockerfile . \
        && docker build -t iqfeedserver-tests -f containers/tests/Dockerfile .

### Validate Syntax

    docker run iqfeedserver-tests syntax

### Running

    docker run -p 9999:9999 okinta/iqfeedserver

### Testing

Make sure iqfeedserver is running. Then run:

    docker run iqfeedserver-tests
