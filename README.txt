This repo extracts information from the power bi api assuming you have a service principal set up with admin priveledges in the pbi api.

There are three main functions:

1) A extraction function that gets data from the api and drops it into a raw s3 bucket

2) A transformation function that gets files from the raw bucket and transforms them using python into a metadata schema

3) A copy machine that will write the file from the s3 staging bucket to redshift


I've parameterized information that is specific to individual situations (like service principal client_id, secret_value, and bucket names) into secrets in aws, so those secrets will need to be set up very specifically with the following key value pairs:

