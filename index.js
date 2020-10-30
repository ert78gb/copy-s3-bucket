import { config } from 'dotenv'
import AWS from 'aws-sdk'
import PQueue from 'p-queue'
import { writeFileSync, readFileSync, existsSync } from 'fs'
import path from 'path'

config()

const resultFilePath = path.join(process.cwd(), 'result.json')
const queue = new PQueue.default({ concurrency: Number.parseInt(process.env.CONCURRENCY, 10) })
const processedMap = new Map()
if (existsSync(resultFilePath)) {
  const processedData = JSON.parse(readFileSync(resultFilePath, { encoding: 'utf-8' }))
  processedData.reduce((map, entry) => {
    map.set(entry.key, entry)

    return map
  }, processedMap)
}

const sourceS3 = new AWS.S3({
  endpoint: process.env.SOURCE_S3_ENDPOINT,
  accessKeyId: process.env.SOURCE_SERVICE_ACCESS_KEY,
  secretAccessKey: process.env.SOURCE_SERVICE_SECRET,
})

const destinationS3 = new AWS.S3({
  endpoint: process.env.DESTINATION_S3_ENDPOINT,
  accessKeyId: process.env.DESTINATION_SERVICE_ACCESS_KEY,
  secretAccessKey: process.env.DESTINATION_SERVICE_SECRET,
})

let nextContinuationToken
let nrOfObjects = 0
let processedObjects = 0

async function sourceBucketLooper (func) {
  while (true) {
    const objects = await sourceS3.listObjectsV2({
      Bucket: process.env.SOURCE_BUCKET,
      ContinuationToken: nextContinuationToken,
    }).promise()

    await func(objects)

    nextContinuationToken = objects.NextContinuationToken

    if (!objects.IsTruncated) {
      nextContinuationToken = undefined
      break
    }
  }

}

async function countSourceObjects (objects) {
  nrOfObjects += objects.KeyCount
}

await sourceBucketLooper(countSourceObjects)

async function copyObjects (objects) {

  for (let i = 0; i < objects.Contents.length; i++)
    queue.add(() => copyObject(objects.Contents[i]))
}

async function copyObject (sourceObject) {
  processedObjects++
  console.log(`${processedObjects}/${nrOfObjects} - ${sourceObject.Key}`)

  const alreadyProcessed = processedMap.get(sourceObject.Key)

  if (alreadyProcessed?.key === sourceObject.Key && !alreadyProcessed?.error)
    return

  const destObjects = await destinationS3.listObjectsV2({
    Bucket: process.env.DESTINATION_BUCKET,
    Prefix: sourceObject.Key,
  }).promise()

  const copy = !destObjects.Contents.some(x => x.Key === sourceObject.Key && x.ETag === sourceObject.ETag)

  if (!copy)
    return

  const headObject = await sourceS3.headObject({
    Bucket: process.env.SOURCE_BUCKET,
    Key: sourceObject.Key,
  }).promise()

  const readStream = sourceS3.getObject({
    Bucket: process.env.SOURCE_BUCKET,
    Key: sourceObject.Key,
  }).createReadStream()

  return destinationS3.upload({
    Bucket: process.env.DESTINATION_BUCKET,
    Key: sourceObject.Key,
    ContentType: headObject.ContentType,
    Metadata: headObject.Metadata,
    Body: readStream,
  }).promise()
    .then(() => {
      processedMap.set(sourceObject.Key, {
        key: sourceObject.Key,
        etag: sourceObject.ETag,
      })
    })
    .catch(error => {
      processedMap.set(sourceObject.Key, {
        key: sourceObject.Key,
        etag: sourceObject.ETag,
        error: error.message,
      })
    })
}

await sourceBucketLooper(copyObjects)
await queue
  .onIdle()
  .finally(()=>{
    writeFileSync(resultFilePath, JSON.stringify(Array.from(processedMap.values())), { encoding: 'utf-8' })
    console.log('Copy finished')
  })
