import { NextResponse } from "next/server";

type ChatRequest = {
  message: string;
  session_id?: string;
};

function extractLastAiTextFromRunResponse(payload: unknown): string {
  if (!payload || typeof payload !== "object") return "";
  const messages = (payload as { messages?: unknown }).messages;
  if (!Array.isArray(messages) || messages.length === 0) return "";
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const m = messages[i] as any;
    if (!m || typeof m !== "object") continue;
    if (m.type !== "ai") continue;
    const c = m.content;
    if (typeof c === "string") return c;
    if (Array.isArray(c)) {
      return c
        .map((part) => {
          if (typeof part === "string") return part;
          if (part && typeof part === "object" && typeof (part as any).text === "string") return (part as any).text;
          return "";
        })
        .filter(Boolean)
        .join("");
    }
  }
  return "";
}

export async function POST(req: Request) {
  const backendUrl =
    process.env.BACKEND_URL || process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:5000";

  let body: ChatRequest;
  try {
    body = (await req.json()) as ChatRequest;
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  const message = (body?.message || "").trim();
  if (!message) {
    return NextResponse.json({ error: "message is required" }, { status: 400 });
  }
  const sessionId = (body?.session_id || "").trim();
  const finalSessionId = sessionId || (typeof crypto?.randomUUID === "function" ? crypto.randomUUID() : "web");

  const res = await fetch(`${backendUrl.replace(/\/$/, "")}/run`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-run-id": finalSessionId
    },
    body: JSON.stringify({
      messages: [{ role: "user", content: message }]
    })
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
      { error: "Backend request failed", status: res.status, body: json ?? text },
      { status: 502 }
    );
  }
  const assistantText = extractLastAiTextFromRunResponse(json);
  return NextResponse.json({ text: assistantText, raw: json });
}
