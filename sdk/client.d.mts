export interface ToolNetSRTJob {
  id: string;
  status: 'queued' | 'processing' | 'completed' | 'failed' | 'cancelled';
  stage: string;
  progress: number;
  completedSegments: number;
  totalSegments: number | null;
  errorCode: string | null;
  resultPath: string | null;
  createdAt?: string;
  updatedAt?: string;
}
export class ToolNetSRTError extends Error {
  status: number;
  details: unknown;
  constructor(message: string, status: number, details: unknown);
}
export class ToolNetSRTClient {
  constructor(options: { baseUrl: string; apiKey: string; fetchImpl?: typeof fetch });
  createJobFromBytes(bytes: BodyInit, mimeType: string, options?: { signal?: AbortSignal }): Promise<ToolNetSRTJob>;
  createJobFromUrl(url: string, options?: { signal?: AbortSignal; idempotencyKey?: string }): Promise<ToolNetSRTJob>;
  getJob(id: string, options?: { signal?: AbortSignal }): Promise<ToolNetSRTJob>;
  getResult(id: string, options?: { signal?: AbortSignal }): Promise<string>;
  cancelJob(id: string, options?: { signal?: AbortSignal }): Promise<ToolNetSRTJob>;
  waitForJob(id: string, options?: { signal?: AbortSignal; intervalMs?: number; maxWaitMs?: number }): Promise<ToolNetSRTJob>;
}
