import {
  DynamoDBClient,
  PutItemCommand,
  QueryCommand,
  DeleteItemCommand,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb'
import * as Y from 'yjs'
import { toBase64 } from 'lib0/dist/buffer.cjs'

const LAST_PART_PLACEHOLDER = '####'
const CACHED_VERSION_POSTFIX = '_cached'

const ddb = new DynamoDBClient({
  apiVersion: '2012-08-10',
  region: process.env.REGION,
  endpoint: process.env.DYNAMODB_ENDPOINT,
})

export async function addConnection(id, docName) {
  console.log('MICHAL: docName', docName)
  await ddb.send(
    new PutItemCommand({
      TableName: process.env.CONNECTIONS_TABLE_NAME,
      Item: {
        PartitionKey: {
          S: id,
        },
        DocName: {
          S: docName,
        },
      },
    })
  )
}

export async function getConnection(id) {
  const { Items } = await ddb.send(
    new QueryCommand({
      TableName: process.env.CONNECTIONS_TABLE_NAME,
      KeyConditionExpression: 'PartitionKey = :partitionkeyval',
      ExpressionAttributeValues: {
        ':partitionkeyval': {
          S: id,
        },
      },
    })
  )

  const connection = Items[0]

  if (!connection) {
    await removeConnection(id)
    throw new Error(`Connection not found: ${id}`)
  }

  return connection
}

export async function getConnectionIds(docName) {
  const { Items } = await ddb.send(
    new QueryCommand({
      TableName: process.env.CONNECTIONS_TABLE_NAME,
      IndexName: 'DocNameIndex',
      KeyConditionExpression: 'DocName = :docnameval',
      ExpressionAttributeValues: {
        ':docnameval': {
          S: docName,
        },
      },
    })
  )
  return Items.map((item) => item.PartitionKey.S)
}

export async function removeConnection(id) {
  await ddb.send(
    new DeleteItemCommand({
      TableName: process.env.CONNECTIONS_TABLE_NAME,
      Key: {
        PartitionKey: {
          S: id,
        },
      },
    })
  )
}

const docPartSplitCharacter = '_part_'
const lastOperationSplitCharacter = '___'

export async function getLastPartFromDoc(docName) {
  const { Items } = await ddb.send(
    new QueryCommand({
      TableName: process.env.DOCS_TABLE_NAME,
      KeyConditionExpression: 'PartitionKey = :partitionkeyval',
      ProjectionExpression: 'LastDocPart',
      ExpressionAttributeValues: {
        ':partitionkeyval': {
          S: docName,
        },
      },
    })
  )

  return (
    (Items && Items[0] && Items[0].LastDocPart && Items[0].LastDocPart.S) ||
    null
  )
}

async function getItemsFromDB (partitionKeyValue) {
  const { Items } = await ddb.send(
    new QueryCommand({
      TableName: process.env.DOCS_TABLE_NAME,
      KeyConditionExpression: 'PartitionKey = :partitionkeyval',
      ExpressionAttributeValues: {
        ':partitionkeyval': {
          S: partitionKeyValue,
        },
      },
    })
  )
  return Items
}

async function createAndGetCachedVersionForThisPart(thisPartKey) {
  const cachedPartitionKeyValue = thisPartKey + CACHED_VERSION_POSTFIX
  const notCachedUpdates = await getItemsFromDB(thisPartKey)
  const { Updates } = notCachedUpdates[0]

  // merge updates and save them
  const updates = Updates.L.map(
    (_) => new Uint8Array(Buffer.from(_.B, 'base64'))
  )

  const mergedUpdate = Y.mergeUpdates(updates)

  await createNewDoc(cachedPartitionKeyValue) // Create empty part placeholder
  await saveUpdateToDb(cachedPartitionKeyValue, mergedUpdate)

  return toBase64(mergedUpdate)
}

function decodeLastDocPartNumber(LastDocPart) {
  if (LastDocPart && LastDocPart.S && LastDocPart.S !== LAST_PART_PLACEHOLDER) {
    const lastPart = Number(LastDocPart.S)
    if (!lastPart) throw new Error('Part should be a number or a placeholder')
    return lastPart
  }
  return LAST_PART_PLACEHOLDER
}

export async function getDocsEvents(
  docName,
  updates = [],
  part = 0,
  lastDocPart = LAST_PART_PLACEHOLDER
) {
  console.log('MICHAL: current part', part)

  let Items  = []
  let mergedUpdates = updates
  const thisPartKey = part ? `${docName}${docPartSplitCharacter}${part}` : docName
  console.log('PINGWING: 143 reading thisPartKey', thisPartKey)

  // if part is 0, then we need to get it from DB to get the last part number to know how many times this function will need to be executed
  if (part === 0) {
    Items = await getItemsFromDB(thisPartKey)
    if (!Items[0]) return null // If the document part 0 does not exists return null, which will result in the document being created in the DB by the calling function

    const { LastDocPart } = Items[0]
    console.log('PINGWING: 172 LastDocPart in part 0', LastDocPart)
    lastDocPart = decodeLastDocPartNumber(LastDocPart)
  }
  console.log('PINGWING: 153 lastDocPart', lastDocPart)

  // if there is a chance to use cache, then check if we have it, if yes just use it, if not create it
  if (lastDocPart > 0 && part < lastDocPart) {
    const cachedPartitionKeyValue = thisPartKey + CACHED_VERSION_POSTFIX
    console.log('PINGWING: 155 cachedPartitionKeyValue', cachedPartitionKeyValue)

    const cachedDataItems = await getItemsFromDB(cachedPartitionKeyValue)
    const weHaveACachedVersion = cachedDataItems && cachedDataItems.length > 0
    console.log('PINGWING: 171 weHaveACachedVersion', weHaveACachedVersion)

    if (weHaveACachedVersion) {
      console.log('PINGWING: 188 using already existing cache for part', part)
      const { Updates } = cachedDataItems[0]
      mergedUpdates = mergedUpdates.concat(Updates.L)

      return await getDocsEvents(docName, mergedUpdates, part + 1, lastDocPart)
    } else  {
      console.log('PINGWING: 194 creating cache for part', part)
      const mergedUpdateBase64 = await createAndGetCachedVersionForThisPart(thisPartKey)
      mergedUpdates = mergedUpdates.concat([{B: mergedUpdateBase64}])

      return await getDocsEvents(docName, mergedUpdates, part + 1, lastDocPart)
    }
  }

  // if we are at the last part then we will not use cache and we need to get the data from DB
  if (part === lastDocPart) {
    Items = await getItemsFromDB(thisPartKey)
  }

  // return routine for part 0 or the last part
  const { Updates } = Items[0]

  mergedUpdates = mergedUpdates.concat(Updates.L)

  // if this is the last part return all the merged parts, this is the main exit point of this recursive function
  if (lastDocPart === LAST_PART_PLACEHOLDER || part === lastDocPart) {
    console.log('PINGWING: 213 lastDocPart === LAST_PART_PLACEHOLDER', lastDocPart === LAST_PART_PLACEHOLDER)
    console.log('PINGWING: 214 part === lastDocPart', part === lastDocPart)
    return {
      Updates: { L: mergedUpdates },
    }
  }

  if (part < lastDocPart) {
    console.log('PINGWING: 221 #####################################');
    console.log('PINGWING: 225 part < lastDocPart', part < lastDocPart)
    console.log('PINGWING: 221 #####################################');
    return await getDocsEvents(docName, mergedUpdates, part + 1, lastDocPart)
  }

  throw new Error('We must have some error in the logic, this should never happen')
}

export async function createNewDoc(docName, withPlaceholder = false) {
  console.log('PINGWING: 213 createNewDoc docName', docName)
  await ddb.send(
    new PutItemCommand({
      TableName: process.env.DOCS_TABLE_NAME,
      Item: {
        PartitionKey: {
          S: docName,
        },
        Updates: {
          L: [],
        },
        ...(withPlaceholder
          ? {
            LastDocPart: {
              S: LAST_PART_PLACEHOLDER,
            },
          }
          : {}),
      },
    })
  )
}

export async function getOrCreateDoc(docName) {
  console.log('MICHAL: STARTING getOrCreateDoc')
  let loadUntilOperation = false
  let docNameToUse = docName
  let loadUntilOperationNo = 0
  if (docName.includes(lastOperationSplitCharacter)) {
    loadUntilOperation = true
    const [docNameId, operationNo] = docName.split(lastOperationSplitCharacter)
    docNameToUse = docNameId
    loadUntilOperationNo = operationNo
  }

  let dbDoc = await getDocsEvents(docNameToUse)
  console.log('MICHAL: dbDoc', dbDoc)

  // Doc not found, create doc
  if (!dbDoc) {
    await createNewDoc(docNameToUse, true) // Create main dog
    await createNewDoc(`${docNameToUse}${docPartSplitCharacter}1`) // Create empty part placeholder
    dbDoc = {
      Updates: { L: [] },
    }
  }

  // @ts-ignore
  const updates = dbDoc.Updates.L.map(
    (_) => new Uint8Array(Buffer.from(_.B, 'base64'))
  )

  const ydoc = new Y.Doc()

  console.log('MICHAL: OPERATIONS COUNT', updates.length)
  let loadUntil = loadUntilOperation ? loadUntilOperationNo : updates.length
  loadUntil = loadUntil > updates.length ? updates.length : loadUntil
  console.log('PINGWING: 121 loadUntil', loadUntil)

  console.log('PINGWING: 220 before merge update')
  const mergedUpdate = Y.mergeUpdates(updates.slice(0, loadUntil))
  console.log('PINGWING: 220 after merge update')

  console.log('PINGWING: 132 before loading all operation to ydoc', loadUntil)
  Y.applyUpdate(ydoc, mergedUpdate)
  console.log('PINGWING: 132 loaded all operations: loadUntil', loadUntil)

  console.log('PINGWING: 296 FINISHING getOrCreateDoc')
  return ydoc
}

const saveUpdateToDb = async (docName, update) => {
  console.log('PINGWING: 287 saveUpdateToDb docName', docName)
  await ddb.send(
    new UpdateItemCommand({
      TableName: process.env.DOCS_TABLE_NAME,
      UpdateExpression: 'SET Updates = list_append(Updates, :attrValue)',
      Key: {
        PartitionKey: {
          S: docName,
        },
      },
      ExpressionAttributeValues: {
        ':attrValue': {
          L: [{ B: update }],
        },
      },
    })
  )
}

const setLastDocPartToDb = async (docName, part) => {
  await ddb.send(
    new UpdateItemCommand({
      TableName: process.env.DOCS_TABLE_NAME,
      UpdateExpression: 'SET LastDocPart = :attrValue',
      Key: {
        PartitionKey: {
          S: docName,
        },
      },
      ExpressionAttributeValues: {
        ':attrValue': {
          S: part,
        },
      },
    })
  )
}

export async function updateDoc(docName, update) {
  let documentPartToWriteTo = await getLastPartFromDoc(docName)

  if (documentPartToWriteTo === null) {
    await setLastDocPartToDb(docName, LAST_PART_PLACEHOLDER)
    await createNewDoc(`${docName}${docPartSplitCharacter}1`) // Create empty part placeholder
    documentPartToWriteTo = LAST_PART_PLACEHOLDER
  }

  let docNameToUse = docName
  if (documentPartToWriteTo !== LAST_PART_PLACEHOLDER) {
    docNameToUse = `${docName}${docPartSplitCharacter}${documentPartToWriteTo}`
  }

  try {
    await saveUpdateToDb(docNameToUse, update)
  } catch (error) {
    console.log('MICHAL: error', error)
    const errorMessage = error.message || error.errorMessage
    if (
      error &&
      errorMessage &&
      errorMessage ===
        'Item size to update has exceeded the maximum allowed size'
    ) {
      const nextDocPart =
        documentPartToWriteTo === LAST_PART_PLACEHOLDER
          ? `${docName}${docPartSplitCharacter}1`
          : `${docName}${docPartSplitCharacter}${
            Number(documentPartToWriteTo) + 1
          }`

      const placeholderDocPart =
        documentPartToWriteTo === LAST_PART_PLACEHOLDER
          ? `${docName}${docPartSplitCharacter}2`
          : `${docName}${docPartSplitCharacter}${
            Number(documentPartToWriteTo) + 2
          }`

      await setLastDocPartToDb(
        docName,
        nextDocPart.split(docPartSplitCharacter)[1]
      )

      await createNewDoc(placeholderDocPart) // Create empty part placeholder
      await saveUpdateToDb(nextDocPart, update)
    }
  }
}
