import { NextResponse } from "next/server";

export async function POST(req: Request) {
  const backendUrl =
    process.env.BACKEND_URL || process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:5000";

  let formData: FormData;
  try {
    formData = await req.formData();
  } catch {
    return NextResponse.json({ error: "Invalid form data" }, { status: 400 });
  }

  const res = await fetch(`${backendUrl.replace(/\/$/, "")}/upload`, {
    method: "POST",
    body: formData
  });

  const text = await res.text();
  let json: unknown = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }

  if (!res.ok) {
    return NextResponse.json(
      { error: "Backend upload failed", status: res.status, body: json ?? text },
      { status: 502 }
    );
  }

  return NextResponse.json(json);
}
