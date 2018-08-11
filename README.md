# A python Project for running background process in Celery with SQS if message size is more then 256KB 
This is example of how to run the celery with the sqs if the  message size is more then 256KB 

## Techs
1. Python
2. Celery
3. AWS SQS, S3

## Description
We know that we could use SQS as the celery broker. Find the more details here <a href="http://docs.celeryproject.org/en/latest/getting-started/brokers/sqs.html">Celery With SQS</a>
But there was a problem, SQS have the meassge size limit to 256KB, if your message size is more then 256 KB, SQS will discard message.

*To resove  above issue done a monkey patching in  <a href="https://github.com/celery/kombu">Kombu</a>
  Resolved the Problem using S3 if size morethen 256 KB putting the message to S3 and then giving the s3 details to SQS. Before
  Excuting the Task getting the message from s3.

   
