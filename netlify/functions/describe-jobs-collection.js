export async function handler() {
  const token = process.env.WEBFLOW_TOKEN;
  const collectionId = process.env.WEBFLOW_COLLECTION_ID;

  const res = await fetch(`https://api.webflow.com/v2/collections/${collectionId}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  });

  const text = await res.text();
  return {
    statusCode: res.ok ? 200 : res.status,
    body: JSON.stringify({ ok: res.ok, status: res.status, body: text }, null, 2),
  };
}