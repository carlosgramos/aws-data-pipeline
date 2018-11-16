# aws-data-pipeline
An AWS Project using S3, Glue, Comprehend, Athena and Quicksight to analyze user sentiment.

The project uses an AWS Glue ETL job to ingest Amazon product reviews (in parquet format)from s3://amazon-reviews-pds/parquet. 
The ETL job extracts the cutomer review text from the record, then a boto3 client is used to send the review text to 
Comprehend for sentiment analysis. Once Comprehend returns the sentiment analysis, the enriched dataset is stored in an S3 bucket.
The dataset is now ready for querying using Athena, or for visualization using Quicksight.

A couple of gotchas:
1) AWS Glue needs an IAM Role with access to the AWS resources you will use for your project. A perfect example is the S3 bucket 
where the enriched data set will be stored. For more information follow this link: https://docs.aws.amazon.com/glue/latest/dg/getting-started-access.html
2) Quicksight will also need IAM persmissions to the S3 bucket where the enriched data set is stored. Failure to do this will 
cause your visualizations to fail. For more information follow this link: https://docs.aws.amazon.com/quicksight/latest/user/troubleshoot-connect-S3.html

This project is based on a project by Roy Hansson at AWS. The project can be found here:
https://aws.amazon.com/blogs/machine-learning/how-to-scale-sentiment-analysis-using-amazon-comprehend-aws-glue-and-amazon-athena/
