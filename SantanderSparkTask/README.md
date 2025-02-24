Repository contains working example of a task for Santander recrutation.

Task was tested based on provided example although it cannot be considered as finished.

Prerequisites:

Some assumption and file changes were made to simplify the processing:
- converting inputs to csv
- removing headers which contained type description
- addind quotes to guarantees structure so it could be parsed as json

Deduplication strategy:

Duplicates were removed based on transaction-id, assuming that the first row in the file should be considered as the one to keep. To ensure deterministic behavior, column row-number to keep original order of rows.

TODO: 
- working unit tests (and testing edge cases)
- more data examples to test, checking against multiple files with multiple dates
- refactor/optimize
- better handling of "rates" value at earlier stage of job, to represent it as decimal
