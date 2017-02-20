# liligo_submission_task README

My task is to compute the mean of the numbers obtained in a Kafka stream in moving windows with the size of 10 seconds.
The stride of the moving window is also 10 seconds.

My solution is based on getting the latest message of the topic.
I compute the time interval of the current moving window. The starting point of the window is the timestamp of the first message, the end point of the interval is computed accordingly.
As the processed message is out of the current window, mean is computed for the numbers and window is moved with 10 seconds and process starts again.

The case when no number is recived in a time windows is also handled.
