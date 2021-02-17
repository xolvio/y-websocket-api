import {
  BatchGetItemCommand,
  DynamoDBClient,
  PutItemCommand,
  QueryCommand,
  DeleteItemCommand,
  UpdateItemCommand,
} from "@aws-sdk/client-dynamodb";
import * as Y from "yjs";

const LAST_PART_PLACEHOLDER = "#########";

const ddb = new DynamoDBClient({
  apiVersion: "2012-08-10",
  region: process.env.REGION,
  endpoint: process.env.DYNAMODB_ENDPOINT,
});

export async function addConnection(id, docName) {
  console.log("MICHAL: docName", docName);
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
  );
}

export async function getConnection(id) {
  const { Items } = await ddb.send(
    new QueryCommand({
      TableName: process.env.CONNECTIONS_TABLE_NAME,
      KeyConditionExpression: "PartitionKey = :partitionkeyval",
      ExpressionAttributeValues: {
        ":partitionkeyval": {
          S: id,
        },
      },
    })
  );

  const connection = Items[0];

  if (!connection) {
    await removeConnection(id);
    throw new Error(`Connection not found: ${id}`);
  }

  return connection;
}

export async function getConnectionIds(docName) {
  const { Items } = await ddb.send(
    new QueryCommand({
      TableName: process.env.CONNECTIONS_TABLE_NAME,
      IndexName: "DocNameIndex",
      KeyConditionExpression: "DocName = :docnameval",
      ExpressionAttributeValues: {
        ":docnameval": {
          S: docName,
        },
      },
    })
  );
  return Items.map((item) => item.PartitionKey.S);
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
  );
}

const docPartSplitCharacter = "_part_";
const lastOperationSplitCharacter = "___";

export async function getLastPartFromDoc(docName) {
  const { Items } = await ddb.send(
    new QueryCommand({
      TableName: process.env.DOCS_TABLE_NAME,
      KeyConditionExpression: "PartitionKey = :partitionkeyval",
      ProjectionExpression: "LastDocPart",
      ExpressionAttributeValues: {
        ":partitionkeyval": {
          S: docName,
        },
      },
    })
  );

  return (
    (Items && Items[0] && Items[0].LastDocPart && Items[0].LastDocPart.S) ||
    null
  );
}

export async function getDocsEvents(
  docName,
  updates = [],
  part = 0,
  lastDocPart = LAST_PART_PLACEHOLDER
) {
  let mergedUpdates = updates;
  const partitionKeyValue = part
    ? `${docName}${docPartSplitCharacter}${part}`
    : docName;
  const { Items } = await ddb.send(
    new QueryCommand({
      TableName: process.env.DOCS_TABLE_NAME,
      KeyConditionExpression: "PartitionKey = :partitionkeyval",
      ExpressionAttributeValues: {
        ":partitionkeyval": {
          S: partitionKeyValue,
        },
      },
    })
  );

  console.log("MICHAL: part", part);

  if (!Items[0]) return null; // If the data if not correct return an empty array

  const { Updates, LastDocPart } = Items[0];
  if (LastDocPart && LastDocPart.S && LastDocPart.S !== LAST_PART_PLACEHOLDER) {
    const lastPart = Number(LastDocPart.S);
    if (!lastPart) throw new Error("Part should be a number or a placeholder");
    lastDocPart = lastPart;
  }

  mergedUpdates = mergedUpdates.concat(Updates.L);
  console.log("MICHAL: LastDocPart", LastDocPart);
  if (lastDocPart === LAST_PART_PLACEHOLDER) {
    return {
      Updates: { L: mergedUpdates },
    };
  }

  if (lastDocPart) {
    if (part === lastDocPart)
      return {
        Updates: { L: mergedUpdates },
      };
    if (part < lastDocPart)
      return await getDocsEvents(docName, mergedUpdates, part + 1, lastDocPart);
  }
}

export async function createNewDoc(docName, withPlaceholder = false) {
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
  );
}

export async function getOrCreateDoc(docName) {
  console.log("MICHAL: 'HEHEHE'", "HEHEHE");
  let loadUntilOperation = false;
  let docNameToUse = docName;
  let loadUntilOperationNo = 0;
  if (docName.includes(lastOperationSplitCharacter)) {
    loadUntilOperation = true;
    const [docNameId, operationNo] = docName.split(lastOperationSplitCharacter);
    docNameToUse = docNameId;
    loadUntilOperationNo = operationNo;
  }

  let dbDoc = await getDocsEvents(docNameToUse);
  console.log("MICHAL: dbDoc", dbDoc);

  // Doc not found, create doc
  if (!dbDoc) {
    await createNewDoc(docNameToUse, true); // Create main dog
    await createNewDoc(`${docNameToUse}${docPartSplitCharacter}1`); // Create empty part placeholder
    dbDoc = {
      Updates: { L: [] },
    };
  }

  // @ts-ignore
  const updates = dbDoc.Updates.L.map(
    (_) => new Uint8Array(Buffer.from(_.B, "base64"))
  );

  const ydoc = new Y.Doc();

  console.log("MICHAL: updates.length", updates.length);
  let loadUntil = loadUntilOperation ? loadUntilOperationNo : updates.length;
  loadUntil = loadUntil > updates.length ? updates.length : loadUntil;
  console.log("PINGWING: 121 loadUntil", loadUntil);

  for (let i = 0; i < loadUntil; i++) {
    Y.applyUpdate(ydoc, updates[i]);
  }

  //ydoc.getArray('content').forEach(c => console.log([...c.entries()]))

  return ydoc;
}

const saveUpdateToDb = async (docName, update) => {
  await ddb.send(
    new UpdateItemCommand({
      TableName: process.env.DOCS_TABLE_NAME,
      UpdateExpression: "SET Updates = list_append(Updates, :attrValue)",
      Key: {
        PartitionKey: {
          S: docName,
        },
      },
      ExpressionAttributeValues: {
        ":attrValue": {
          L: [{ B: update }],
        },
      },
    })
  );
};

const setLastDocPartToDb = async (docName, part) => {
  await ddb.send(
    new UpdateItemCommand({
      TableName: process.env.DOCS_TABLE_NAME,
      UpdateExpression: "SET LastDocPart = :attrValue",
      Key: {
        PartitionKey: {
          S: docName,
        },
      },
      ExpressionAttributeValues: {
        ":attrValue": {
          S: part,
        },
      },
    })
  );
};

export async function updateDoc(docName, update) {
  let documentPartToWriteTo = await getLastPartFromDoc(docName);

  if (documentPartToWriteTo === null) {
    await setLastDocPartToDb(docName, LAST_PART_PLACEHOLDER);
    await createNewDoc(`${docName}${docPartSplitCharacter}1`); // Create empty part placeholder
    documentPartToWriteTo = LAST_PART_PLACEHOLDER;
  }

  let docNameToUse = docName;
  if (documentPartToWriteTo !== LAST_PART_PLACEHOLDER) {
    docNameToUse = `${docName}${docPartSplitCharacter}${documentPartToWriteTo}`;
  }

  try {
    await saveUpdateToDb(docNameToUse, update);
  } catch (error) {
    console.log("MICHAL: error", error);
    const errorMessage = error.message || error.errorMessage;
    if (
      error &&
      errorMessage &&
      errorMessage ===
        "Item size to update has exceeded the maximum allowed size"
    ) {
      const nextDocPart =
        documentPartToWriteTo === LAST_PART_PLACEHOLDER
          ? `${docName}${docPartSplitCharacter}1`
          : `${docName}${docPartSplitCharacter}${
              Number(documentPartToWriteTo) + 1
            }`;

      const placeholderDocPart =
        documentPartToWriteTo === LAST_PART_PLACEHOLDER
          ? `${docName}${docPartSplitCharacter}2`
          : `${docName}${docPartSplitCharacter}${
              Number(documentPartToWriteTo) + 2
            }`;

      await setLastDocPartToDb(
        docName,
        nextDocPart.split(docPartSplitCharacter)[1]
      );

      await createNewDoc(placeholderDocPart); // Create empty part placeholder
      await saveUpdateToDb(nextDocPart, update);
    }
  }
}
