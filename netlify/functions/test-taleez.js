export async function handler() {
  const base = process.env.TALEEZ_BASE_URL || "https://api.taleez.com";
  const path = process.env.TALEEZ_OFFERS_PATH;
  const secret = process.env.TALEEZ_API_SECRET;

  if (!path || !secret) {
    return {
      statusCode: 400,
      body: JSON.stringify({
        ok: false,
        missing: {
          TALEEZ_OFFERS_PATH: !path,
          TALEEZ_API_SECRET: !secret,
        },
      }),
    };
  }

  const url = new URL(`${base}${path}`);
  // paging params are supported on many endpoints; harmless if ignored
  url.searchParams.set("page", "0");
  url.searchParams.set("pageSize", "1");

  const res = await fetch(url.toString(), {
    headers: {
      "X-taleez-api-secret": secret,
      "Content-Type": "application/json",
    },
  });

  const text = await res.text();

  return {
    statusCode: res.ok ? 200 : res.status,
    body: JSON.stringify({
      ok: res.ok,
      status: res.status,
      preview: text.slice(0, 700),
    }),
  };
}