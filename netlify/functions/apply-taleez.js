// netlify/functions/apply-taleez.js

export async function handler(event) {
    // 1. Define your CORS headers
    const headers = {
      "Access-Control-Allow-Origin": "https://brawo.webflow.io", // Allows only your Webflow site. Use "*" to allow all domains.
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "POST, OPTIONS"
    };
  
    // 2. Catch the preflight OPTIONS request from the browser
    if (event.httpMethod === "OPTIONS") {
      return {
        statusCode: 200,
        headers,
        body: "CORS preflight successful"
      };
    }
  
    // Guard against non-POST requests
    if (event.httpMethod !== "POST") {
      return { 
        statusCode: 405, 
        headers, 
        body: JSON.stringify({ error: "Method Not Allowed" }) 
      };
    }
  
    try {
      const data = JSON.parse(event.body);
      const { taleezId, firstName, lastName, email, phone } = data;
  
      // Validate required fields
      if (!taleezId || !firstName || !lastName || !email) {
        return { 
          statusCode: 400, 
          headers, // Include headers in every response
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
          headers, // Include headers
          body: JSON.stringify({ error: "Failed to submit application to Taleez." }),
        };
      }
  
      return {
        statusCode: 200,
        headers, // Include headers
        body: JSON.stringify({ success: true, message: "Application submitted successfully!" }),
      };
  
    } catch (err) {
      console.error("Apply Error:", err);
      return { 
        statusCode: 500, 
        headers, // Include headers
        body: JSON.stringify({ error: "Internal Server Error" }) 
      };
    }
  }