import { app, InvocationContext } from "@azure/functions";
import { TableClient, AzureNamedKeyCredential } from "@azure/data-tables";

const storageAccountName = process.env.storageAccountName;
const accountKey = process.env.TABLE_STORAGE_ACCOUNT_KEY;

const tableName = process.env.tableName;

const tableClient = new TableClient(
  `https://${storageAccountName}.table.core.windows.net`,
  tableName,
  new AzureNamedKeyCredential(storageAccountName, accountKey)
);

async function createTableIfNotExists() {
  try {
    await tableClient.createTable();
    console.log(`✅ Table '${tableName}' is ready.`);
  } catch (error) {
    if (error.statusCode !== 409) {
      console.error("❌ Error creating table:", error);
    }
  }
}
createTableIfNotExists();

export async function storageQueueTrigger1(
  queueItem: any,
  context: InvocationContext
): Promise<void> {
  context.log("🔹 Processing queue message:", queueItem);

  // Convert queue message to Table Storage format
  const entity = {
    partitionKey: "ProcessedMessages", // Group messages under one partition
    rowKey: new Date().getTime().toString(), // Unique timestamp as RowKey
    messageData: JSON.stringify(queueItem),
    processedAt: new Date().toISOString(),
  };

  // Store message in Table Storage
  try {
    await tableClient.createEntity(entity);
    context.log("✅ Message stored in Azure Table Storage:", entity);
  } catch (error) {
    context.log("❌ Error storing message:", error);
  }
}

app.storageQueue("storageQueueTrigger1", {
  queueName: "js-queue-items",
  connection: "generalstorageaubair_STORAGE", // Ensure this is configured in local.settings.json
  handler: storageQueueTrigger1,
});
