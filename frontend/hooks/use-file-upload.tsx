import { useState, useRef, useEffect, ChangeEvent } from "react";
import { toast } from "sonner";
import { ContentBlock } from "@langchain/core/messages";

export const SUPPORTED_FILE_TYPES = [
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  "text/csv",
];

interface UseFileUploadOptions {
  initialBlocks?: ContentBlock.Multimodal.Data[];
}

export function useFileUpload({
  initialBlocks = [],
}: UseFileUploadOptions = {}) {
  const [contentBlocks, setContentBlocks] =
    useState<ContentBlock.Multimodal.Data[]>(initialBlocks);
  const dropRef = useRef<HTMLDivElement>(null);
  const [dragOver, setDragOver] = useState(false);
  const dragCounter = useRef(0);

  const isDuplicate = (file: File, blocks: ContentBlock.Multimodal.Data[]) => {
    return blocks.some((b: any) => b?.metadata?.name === file.name);
  };

  const uploadFiles = async (files: File[]) => {
    const fd = new FormData();
    files.forEach((f) => fd.append("files", f));
    const res = await fetch("/api/upload", { method: "POST", body: fd });
    const data = (await res.json()) as {
      files?: Array<{ original_name: string; saved_path: string }>;
      error?: string;
    };
    if (!res.ok) throw new Error(data?.error || `Upload failed (${res.status})`);
    return data.files || [];
  };

  const handleFileUpload = async (e: ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (!files) return;
    const fileArray = Array.from(files);
    const validFiles = fileArray.filter(
      (file) =>
        SUPPORTED_FILE_TYPES.includes(file.type) ||
        file.name.toLowerCase().endsWith(".xlsx") ||
        file.name.toLowerCase().endsWith(".xlsm") ||
        file.name.toLowerCase().endsWith(".csv"),
    );
    const invalidFiles = fileArray.filter(
      (file) => !validFiles.includes(file),
    );
    const duplicateFiles = validFiles.filter((file) =>
      isDuplicate(file, contentBlocks),
    );
    const uniqueFiles = validFiles.filter(
      (file) => !isDuplicate(file, contentBlocks),
    );

    if (invalidFiles.length > 0) {
      toast.error(
        "不支持的文件类型。请上传 Excel(xlsx/xlsm) 或 CSV 文件。",
      );
    }
    if (duplicateFiles.length > 0) {
      toast.error(
        `Duplicate file(s) detected: ${duplicateFiles.map((f) => f.name).join(", ")}. Each file can only be uploaded once per message.`,
      );
    }

    if (uniqueFiles.length) {
      try {
        const uploaded = await uploadFiles(uniqueFiles);
        const newBlocks = uploaded
          .filter((f) => f.saved_path)
          .map(
            (f) =>
              ({
                type: "text",
                text: f.saved_path,
                metadata: { name: f.original_name },
              }) as any,
          );
        setContentBlocks((prev) => [...prev, ...newBlocks]);
      } catch (err) {
        toast.error(err instanceof Error ? err.message : String(err));
      }
    }
    e.target.value = "";
  };

  // Drag and drop handlers
  useEffect(() => {
    if (!dropRef.current) return;

    // Global drag events with counter for robust dragOver state
    const handleWindowDragEnter = (e: DragEvent) => {
      if (e.dataTransfer?.types?.includes("Files")) {
        dragCounter.current += 1;
        setDragOver(true);
      }
    };
    const handleWindowDragLeave = (e: DragEvent) => {
      if (e.dataTransfer?.types?.includes("Files")) {
        dragCounter.current -= 1;
        if (dragCounter.current <= 0) {
          setDragOver(false);
          dragCounter.current = 0;
        }
      }
    };
    const handleWindowDrop = async (e: DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      dragCounter.current = 0;
      setDragOver(false);

      if (!e.dataTransfer) return;

      const files = Array.from(e.dataTransfer.files);
      const validFiles = files.filter(
        (file) =>
          SUPPORTED_FILE_TYPES.includes(file.type) ||
          file.name.toLowerCase().endsWith(".xlsx") ||
          file.name.toLowerCase().endsWith(".xlsm") ||
          file.name.toLowerCase().endsWith(".csv"),
      );
      const invalidFiles = files.filter(
        (file) => !validFiles.includes(file),
      );
      const duplicateFiles = validFiles.filter((file) =>
        isDuplicate(file, contentBlocks),
      );
      const uniqueFiles = validFiles.filter(
        (file) => !isDuplicate(file, contentBlocks),
      );

      if (invalidFiles.length > 0) {
        toast.error(
          "不支持的文件类型。请上传 Excel(xlsx/xlsm) 或 CSV 文件。",
        );
      }
      if (duplicateFiles.length > 0) {
        toast.error(
          `Duplicate file(s) detected: ${duplicateFiles.map((f) => f.name).join(", ")}. Each file can only be uploaded once per message.`,
        );
      }

      if (uniqueFiles.length) {
        try {
          const uploaded = await uploadFiles(uniqueFiles);
          const newBlocks = uploaded
            .filter((f) => f.saved_path)
            .map(
              (f) =>
                ({
                  type: "text",
                  text: f.saved_path,
                  metadata: { name: f.original_name },
                }) as any,
            );
          setContentBlocks((prev) => [...prev, ...newBlocks]);
        } catch (err) {
          toast.error(err instanceof Error ? err.message : String(err));
        }
      }
    };
    const handleWindowDragEnd = (e: DragEvent) => {
      dragCounter.current = 0;
      setDragOver(false);
    };
    window.addEventListener("dragenter", handleWindowDragEnter);
    window.addEventListener("dragleave", handleWindowDragLeave);
    window.addEventListener("drop", handleWindowDrop);
    window.addEventListener("dragend", handleWindowDragEnd);

    // Prevent default browser behavior for dragover globally
    const handleWindowDragOver = (e: DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
    };
    window.addEventListener("dragover", handleWindowDragOver);

    // Remove element-specific drop event (handled globally)
    const handleDragOver = (e: DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      setDragOver(true);
    };
    const handleDragEnter = (e: DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      setDragOver(true);
    };
    const handleDragLeave = (e: DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      setDragOver(false);
    };
    const element = dropRef.current;
    element.addEventListener("dragover", handleDragOver);
    element.addEventListener("dragenter", handleDragEnter);
    element.addEventListener("dragleave", handleDragLeave);

    return () => {
      element.removeEventListener("dragover", handleDragOver);
      element.removeEventListener("dragenter", handleDragEnter);
      element.removeEventListener("dragleave", handleDragLeave);
      window.removeEventListener("dragenter", handleWindowDragEnter);
      window.removeEventListener("dragleave", handleWindowDragLeave);
      window.removeEventListener("drop", handleWindowDrop);
      window.removeEventListener("dragend", handleWindowDragEnd);
      window.removeEventListener("dragover", handleWindowDragOver);
      dragCounter.current = 0;
    };
  }, [contentBlocks]);

  const removeBlock = (idx: number) => {
    setContentBlocks((prev) => prev.filter((_, i) => i !== idx));
  };

  const resetBlocks = () => setContentBlocks([]);

  /**
   * Handle paste event for files (images, PDFs)
   * Can be used as onPaste={handlePaste} on a textarea or input
   */
  const handlePaste = async (
    e: React.ClipboardEvent<HTMLTextAreaElement | HTMLInputElement>,
  ) => {
    const items = e.clipboardData.items;
    if (!items) return;
    const files: File[] = [];
    for (let i = 0; i < items.length; i += 1) {
      const item = items[i];
      if (item.kind === "file") {
        const file = item.getAsFile();
        if (file) files.push(file);
      }
    }
    if (files.length === 0) {
      return;
    }
    e.preventDefault();
    const validFiles = files.filter(
      (file) =>
        SUPPORTED_FILE_TYPES.includes(file.type) ||
        file.name.toLowerCase().endsWith(".xlsx") ||
        file.name.toLowerCase().endsWith(".xlsm") ||
        file.name.toLowerCase().endsWith(".csv"),
    );
    const invalidFiles = files.filter(
      (file) => !validFiles.includes(file),
    );
    const isDuplicate = (file: File) => {
      return contentBlocks.some((b: any) => b?.metadata?.name === file.name);
    };
    const duplicateFiles = validFiles.filter(isDuplicate);
    const uniqueFiles = validFiles.filter((file) => !isDuplicate(file));
    if (invalidFiles.length > 0) {
      toast.error(
        "不支持的文件类型。请粘贴 Excel(xlsx/xlsm) 或 CSV 文件。",
      );
    }
    if (duplicateFiles.length > 0) {
      toast.error(
        `Duplicate file(s) detected: ${duplicateFiles.map((f) => f.name).join(", ")}. Each file can only be uploaded once per message.`,
      );
    }
    if (uniqueFiles.length > 0) {
      try {
        const uploaded = await uploadFiles(uniqueFiles);
        const newBlocks = uploaded
          .filter((f) => f.saved_path)
          .map(
            (f) =>
              ({
                type: "text",
                text: f.saved_path,
                metadata: { name: f.original_name },
              }) as any,
          );
        setContentBlocks((prev) => [...prev, ...newBlocks]);
      } catch (err) {
        toast.error(err instanceof Error ? err.message : String(err));
      }
    }
  };

  return {
    contentBlocks,
    setContentBlocks,
    handleFileUpload,
    dropRef,
    removeBlock,
    resetBlocks,
    dragOver,
    handlePaste,
  };
}
