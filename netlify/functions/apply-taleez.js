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
    // Destructure cvData and cvName that are sent from your Webflow script
    const { taleezId, firstName, lastName, email, phone, cvData, cvName } = data;

    // Validate required fields
    if (!taleezId || !firstName || !lastName || !email) {
      return { 
        statusCode: 400, 
        headers, 
        body: JSON.stringify({ error: "Missing required fields" }) 
      };
    }

    const secret = process.env.TALEEZ_API_SECRET;
    
    // ==========================================
    // STEP 1: CREATE THE APPLICATION
    // ==========================================
    const url = `https://api.taleez.com/0/jobs/${taleezId}/applications`;

    const payload = {
      firstName,
      lastName,
      email,
      phone: phone || "",
      initialReferrer: "Brawo Webflow Website", // Custom tracking tag
      bypassRequiredQuestions: true
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
        headers,
        body: JSON.stringify({ error: "Failed to submit application to Taleez." }),
      };
    }

    // Parse the response to get the newly created candidate's info
    const candidateData = await res.json();
    
    // Depending on the exact Taleez response structure, the ID might be direct or nested.
    // This safely catches it either way.
    const candidateId = candidateData.candidateId || candidateData.id;

    // ==========================================
    // STEP 2: UPLOAD THE CV (If provided)
    // ==========================================
    if (cvData && cvName && candidateId) {
      // Convert the Base64 string from Webflow back into a readable binary Buffer
      const cvBuffer = Buffer.from(cvData, "base64");
      
      // Create a Blob from the buffer (Node 18+ native support)
      const fileBlob = new Blob([cvBuffer]);
      
      // Build the multipart/form-data payload
      const formData = new FormData();
      formData.append("file", fileBlob, cvName);

      // Upload to the specific candidate's document endpoint.
      // ?cv=true flags this document specifically as their main resume in Taleez.
      const documentRes = await fetch(`https://api.taleez.com/0/candidates/${candidateId}/documents?cv=true`, {
        method: "POST",
        headers: {
          "X-taleez-api-secret": secret,
          // IMPORTANT: Do NOT manually set 'Content-Type' to 'multipart/form-data' here.
          // Native fetch handles the boundary headers automatically when passing FormData.
        },
        body: formData,
      });

      if (!documentRes.ok) {
        const errorText = await documentRes.text();
        console.error("Failed to upload CV to Taleez:", errorText);
        // Note: We don't throw an error here because the application itself was successful.
        // We just log the CV failure so you can track it in Netlify logs.
      }
    }

    // ==========================================
    // STEP 3: RETURN SUCCESS TO WEBFLOW
    // ==========================================
    return {
      statusCode: 200,
      headers, 
      body: JSON.stringify({ success: true, message: "Application and CV submitted successfully!" }),
    };

  } catch (err) {
    console.error("Apply Error:", err);
    return { 
      statusCode: 500, 
      headers, 
      body: JSON.stringify({ error: "Internal Server Error" }) 
    };
  }
}