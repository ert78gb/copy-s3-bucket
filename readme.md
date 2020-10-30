# Copy S3 bucket from A service provider to B

There are many cloud provider who provide storage service that API is AWS S3 compatible.

This script helps in the migration. It uses streams to copy from one bucket to the other, so the files would be store on the migrator device.

The script overwrites the already existing object in the destination bucket if the ETag property is not match.
If the ETags are matches then skips the copy.

Support parallel copy of objects.

Copy:
 - Object content
 - ContentType
 - Metadata

## How to use

Compatible with Node 15+

1) Clone the repository `$ git clone https://github.com/ert78gb/copy-s3-bucket.git`
2) Switch working directory `$ cd copy-s3-bucket`
2) Install dependencies `$ npm ci`
3) Copy the `.env-example` file as `.env` and fill the connection information. `$ cp .env-example .env`
4) Start the script `$ node index.js`
5) Check the `result.json` in the working directory. Is it contains any error or not. If it contains then repeat from step 5. The script try copy only the objects that are missing from the destination bucket or has error in the `result.json` 

## TODO

- Better progress bar
- Better final report
