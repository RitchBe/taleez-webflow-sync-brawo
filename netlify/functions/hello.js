export async function handler() {
  return {
    statusCode: 200,
    body: JSON.stringify({ ok: true, message: "Netlify ifunctions are working" }),
  };
}