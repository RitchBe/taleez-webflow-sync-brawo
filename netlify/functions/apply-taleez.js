// netlify/functions/apply-taleez.js

export async function handler(event) {
  // 1. Define your CORS headers
  const headers = {
    "Access-Control-Allow-Origin": "https://brawo.webflow.io", 
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
    const { taleezId, firstName, lastName, email, phone, cvData, cvName } = data;

    // Validate required fields (Removed taleezId from here)
    if (!firstName || !lastName || !email) {
      return { 
        statusCode: 400, 
        headers, 
        body: JSON.stringify({ error: "Missing required fields" }) 
      };
    }

    const secret = process.env.TALEEZ_API_SECRET;
    
    // ==========================================
    // STEP 1: ROUTE AND CREATE THE APPLICATION
    // ==========================================
    
    // Determine if this is a spontaneous application based on the presence of taleezId
    const isSpontaneous = !taleezId || taleezId.trim() === "";
    
    const url = isSpontaneous 
      ? `https://api.taleez.com/0/spontaneous/applications` 
      : `https://api.taleez.com/0/jobs/${taleezId}/applications`;

    const payload = {
      firstName,
      lastName,
      email,
      phone: phone || "",
      initialReferrer: isSpontaneous ? "Brawo Webflow - Spontaneous" : "Brawo Webflow - Job Board",
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

    const candidateData = await res.json();
    const candidateId = candidateData.candidateId || candidateData.id;

    // ==========================================
    // STEP 2: UPLOAD THE CV (If provided)
    // ==========================================
    if (cvData && cvName && candidateId) {
      const cvBuffer = Buffer.from(cvData, "base64");
      const fileBlob = new Blob([cvBuffer]);
      
      const formData = new FormData();
      formData.append("file", fileBlob, cvName);

      const documentRes = await fetch(`https://api.taleez.com/0/candidates/${candidateId}/documents?cv=true`, {
        method: "POST",
        headers: {
          "X-taleez-api-secret": secret,
        },
        body: formData,
      });

      if (!documentRes.ok) {
        const errorText = await documentRes.text();
        console.error("Failed to upload CV to Taleez:", errorText);
      }
    }

    // ==========================================
    // STEP 3: RETURN SUCCESS TO WEBFLOW
    // ==========================================
    return {
      statusCode: 200,
      headers, 
      body: JSON.stringify({ success: true, message: "Application submitted successfully!" }),
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