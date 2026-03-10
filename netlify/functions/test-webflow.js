export async function handler() {
  const token = process.env.WEBFLOW_TOKEN;
  const collectionId = process.env.WEBFLOW_COLLECTION_ID;

  if (!token || !collectionId) {
    return {
      statusCode: 400,
      body: JSON.stringify({
        ok: false,
        missing: { WEBFLOW_TOKEN: !token, WEBFLOW_COLLECTION_ID: !collectionId },
      }),
    };
  }

  const res = await fetch(
    `https://api.webflow.com/v2/collections/${collectionId}/items?limit=1`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
    }
  );

  const text = await res.text();

  return {
    statusCode: res.ok ? 200 : res.status,
    body: JSON.stringify({
      ok: res.ok,
      status: res.status,
      preview: text.slice(0, 500),
    }),
  };
}