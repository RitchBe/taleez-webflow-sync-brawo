export async function handler() {
  const required = ["WEBFLOW_TOKEN","WEBFLOW_COLLECTION_ID","TALEEZ_USER","TALEEZ_PASS","TALEEZ_BASE_URL"];
  const missing = required.filter(k => !process.env[k]);
  return {
    statusCode: missing.length ? 400 : 200,
    body: JSON.stringify({ ok: missing.length === 0, missing })
  };
}