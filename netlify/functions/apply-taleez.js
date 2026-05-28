// netlify/functions/apply-taleez.js

export async function handler(event) {
    // Guard against non-POST requests
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: "Method Not Allowed" };
    }
  
    try {
      const data = JSON.parse(event.body);
      const { taleezId, firstName, lastName, email, phone } = data;
  
      // Validate required fields
      if (!taleezId || !firstName || !lastName || !email) {
        return { 
          statusCode: 400, 
          body: JSON.stringify({ error: "Missing required fields" }) 
        };
      }
  
      const secret = process.env.TALEEZ_API_SECRET;
      // Taleez endpoint to create an application for a specific job
      const url = `https://api.taleez.com/0/jobs/${taleezId}/applications`;
  
      const payload = {
        firstName,
        lastName,
        email,
        phone: phone || "",
        initialReferrer: "Brawo Webflow Website" // Custom tracking tag
      };
  
      const res = await fetch(url, {
        method: "POST",
        headers: {
          "X-taleez-api-secret": secret,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });
  
      if (!res.ok) {
        const text = await res.text();
        console.error("Taleez application error:", text);
        return {
          statusCode: res.status,
          body: JSON.stringify({ error: "Failed to submit application to Taleez." }),
        };
      }
  
      return {
        statusCode: 200,
        body: JSON.stringify({ success: true, message: "Application submitted successfully!" }),
      };
  
    } catch (err) {
      console.error("Apply Error:", err);
      return { statusCode: 500, body: JSON.stringify({ error: "Internal Server Error" }) };
    }
  }