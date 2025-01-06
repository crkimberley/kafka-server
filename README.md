Kafka Server - using assign & seek

To maximise performance, the JSON objects are kept as JSON and passed on, 
rather than being converted to case classes and back.

If validation, transformation or cleaning up of data were required, 
then this would be done using case classes.