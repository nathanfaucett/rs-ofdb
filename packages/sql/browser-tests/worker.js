import { Database } from "../browser/index.js";

self.onmessage = async ({ data: name }) => {
  let db;
  let client;
  try {
    db = await Database.open(name);
    client = db.client();
    const tx = await client.transaction();
    try {
      await tx.execute("INSERT INTO items VALUES (7, 'worker')");
      await tx.commit();
    } finally {
      tx.close();
    }
    self.postMessage(await client.execute("SELECT id,text FROM items"));
  } catch (error) {
    self.postMessage({ error: String(error) });
  } finally {
    client?.close();
    db?.close();
  }
};
