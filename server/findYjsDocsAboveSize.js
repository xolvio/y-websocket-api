import { DynamoDBClient, ScanCommand } from "@aws-sdk/client-dynamodb";

const ddb = new DynamoDBClient({
  apiVersion: "2012-08-10",
  region: "us-east-1",
  endpoint: "https://dynamodb.us-east-1.amazonaws.com",
  credentials: {
    accessKeyId: process.env.ACCESS_KEY_ID,
    secretAccessKey: process.env.ACCESS_KEY_SECRET,
  },
});

(async () => {
  const scanMe = async (aboveSize = 200000, items = [], lastKey = "") => {
    const { Items, LastEvaluatedKey } = await ddb.send(
      new ScanCommand({
        TableName: "docs",
        AttributesToGet: ["PartitionKey", "Updates"],
        //Limit: 5,
        ...(lastKey ? { ExclusiveStartKey: lastKey } : {}),
      })
    );

    lastKey = LastEvaluatedKey;

    items = items.concat(
      Items.map((i) => ({
        PartitionKey: i.PartitionKey,
        size: i.Updates.L.reduce((acc, attr) => acc + attr.B.byteLength, 0),
      })).filter((i) => i.size > aboveSize)
    );

    if (lastKey) await scanMe(aboveSize, items, lastKey);
    else console.log(items.sort((a, b) => a.size - b.size));
  };

  await scanMe(200000);
})();
