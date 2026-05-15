import React, { createContext, ReactNode, useContext, useEffect, useRef, useState } from "react";
import { useQueryState } from "nuqs";
import { v4 as uuidv4 } from "uuid";
import { type Message } from "@langchain/langgraph-sdk";

type StreamContextType = {
  messages: Message[];
  values: { messages: Message[]; ui?: any[] };
  isLoading: boolean;
  error: unknown;
  interrupt: unknown;
  getMessagesMetadata: (message: Message) => any;
  setBranch: (branch: string) => void;
  submit: (
    input?: unknown,
    _options?: { optimisticValues?: (prev: any) => any; [key: string]: any },
  ) => void;
  stop: () => void;
};

const StreamContext = createContext<StreamContextType | undefined>(undefined);

function contentToText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";

  return content
    .map((part: any) => {
      if (!part || typeof part !== "object") return "";
      if (part.type === "text" && typeof part.text === "string") return part.text;
      if (part.type === "image_url" && typeof part.image_url?.url === "string") return part.image_url.url;
      if (part.type === "file_url" && typeof part.file_url?.url === "string") return part.file_url.url;
      if (part.type === "file" && typeof part.metadata?.filename === "string") return part.metadata.filename;
      if (typeof part.text === "string") return part.text;
      return "";
    })
    .filter(Boolean)
    .join("\n");
}

function buildUserMessageText(messages: Message[]): string {
  const lastHuman = [...messages].reverse().find((m) => m.type === "human");
  if (!lastHuman) return "";
  return contentToText((lastHuman as any).content);
}

export const StreamProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [threadId, setThreadId] = useQueryState("threadId");
  const [sessionId, setSessionId] = useState<string>(() => threadId || "");
  useEffect(() => {
    if (threadId) {
      setSessionId(threadId);
      return;
    }
    const id = uuidv4();
    setThreadId(id);
    setSessionId(id);
  }, [threadId, setThreadId]);

  const [messages, setMessages] = useState<Message[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const abortRef = useRef<AbortController | null>(null);

  const stop = () => {
    abortRef.current?.abort();
    abortRef.current = null;
    setIsLoading(false);
  };

  const submit: StreamContextType["submit"] = async (input) => {
    const maybeMessages = (input as any)?.messages;
    const nextMessages = Array.isArray(maybeMessages) ? maybeMessages : messages;
    const text = buildUserMessageText(nextMessages);
    if (!text.trim()) return;

    stop();
    const abort = new AbortController();
    abortRef.current = abort;

    setError(null);
    setIsLoading(true);
    setMessages(nextMessages);

    try {
      const sid = sessionId || threadId || uuidv4();
      if (!threadId) setThreadId(sid);
      if (!sessionId) setSessionId(sid);
      const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:5000";
      const aiId = uuidv4();
      const placeholderAi: Message = { id: aiId, type: "ai", content: "" } as any;
      setMessages((prev) => [...prev, placeholderAi]);

      const res = await fetch(`${backendUrl}/stream_run`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-run-id": sid
        },
        body: JSON.stringify({ messages: [{ role: "user", content: text }] }),
        signal: abort.signal
      });

      if (!res.ok || !res.body) {
        const data = (await res.text());
        setError(data || "Backend request failed");
        setMessages((prev) => prev.filter((m) => m.id !== aiId));
        return;
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let lastText = "";

      while (!abort.signal.aborted) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        while (true) {
          const idx = buffer.indexOf("\n\n");
          if (idx === -1) break;
          const rawEvent = buffer.slice(0, idx);
          buffer = buffer.slice(idx + 2);

          const dataLines = rawEvent
            .split("\n")
            .filter((l) => l.startsWith("data:"))
            .map((l) => l.slice(5).trim());
          if (dataLines.length === 0) continue;

          try {
            const chunk = JSON.parse(dataLines.join("\n"));
            const messages = (chunk as any)?.messages;
            if (!Array.isArray(messages)) continue;
            for (let i = messages.length - 1; i >= 0; i -= 1) {
              const m = messages[i];
              if (m?.type === "ai" && typeof m?.content === "string") {
                lastText = m.content;
                break;
              }
            }
          } catch {
            // skip unparseable chunks
          }

          setMessages((prev) =>
            prev.map((m) => (m.id === aiId ? ({ ...m, content: lastText } as any) : m)),
          );
        }
      }

      if (!lastText.trim()) {
        setMessages((prev) => prev.filter((m) => m.id !== aiId));
      }
    } catch (e) {
      if ((e as any)?.name !== "AbortError") setError(e);
    } finally {
      setIsLoading(false);
    }
  };

  const value: StreamContextType = {
    messages,
    values: { messages, ui: [] },
    isLoading,
    error,
    interrupt: null,
    getMessagesMetadata: () => ({
      firstSeenState: { parent_checkpoint: null, values: { messages } },
      branch: undefined,
      branchOptions: undefined
    }),
    setBranch: () => {},
    submit,
    stop
  };

  return <StreamContext.Provider value={value}>{children}</StreamContext.Provider>;
};

export const useStreamContext = (): StreamContextType => {
  const context = useContext(StreamContext);
  if (!context) throw new Error("useStreamContext must be used within a StreamProvider");
  return context;
};

export default StreamContext;
