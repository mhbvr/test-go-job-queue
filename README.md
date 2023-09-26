Micro benchmarks of implementations of job execution engine.

Messages should be received and send in order. Every message should be processed, but order of processing is not important, so it could be done in parallel.

The main difficulty is to preserve an order of messages after processing.