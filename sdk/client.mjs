/** ToolNet-SRT REST client (Node.js >= 20 or modern browser). SRT-only by design. */
export class ToolNetSRTError extends Error {
  constructor(message, status, details) {
    super(message);
    this.name = 'ToolNetSRTError';
    this.status = status;
    this.details = details;
  }
}

export class ToolNetSRTClient {
  constructor({ baseUrl, apiKey, fetchImpl = fetch } = {}) {
    if (!baseUrl || !/^https?:\/\//.test(baseUrl)) throw new Error('baseUrl must be an HTTP(S) URL');
    if (!apiKey) throw new Error('apiKey is required');
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.apiKey = apiKey;
    this.fetch = fetchImpl;
  }

  async #request(path, { method = 'GET', headers = {}, body, signal, output = 'json' } = {}) {
    const response = await this.fetch(this.baseUrl + path, {
      method,
      headers: { 'X-API-Key': this.apiKey, ...headers },
      body,
      signal,
    });
    if (!response.ok) {
      const text = await response.text();
      let details = text;
      try { details = JSON.parse(text); } catch {}
      throw new ToolNetSRTError(`HTTP ${response.status} from ToolNet-SRT`, response.status, details);
    }
    if (output === 'text') return response.text();
    return response.json();
  }

  async createJobFromBytes(bytes, mimeType, { signal } = {}) {
    if (!bytes || !mimeType) throw new Error('bytes and mimeType are required');
    return this.#request('/v1/jobs', {
      method: 'POST', headers: { 'Content-Type': mimeType },
      body: bytes, signal,
    });
  }

  async createJobFromUrl(url, { signal } = {}) {
    if (!url) throw new Error('url is required');
    return this.#request('/v1/jobs/url', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url }), signal,
    });
  }

  getJob(id, { signal } = {}) {
    return this.#request(`/v1/jobs/${encodeURIComponent(id)}`, { signal });
  }

  getResult(id, { signal } = {}) {
    return this.#request(`/v1/jobs/${encodeURIComponent(id)}/result`, { signal, output: 'text' });
  }

  cancelJob(id, { signal } = {}) {
    return this.#request(`/v1/jobs/${encodeURIComponent(id)}`, { method: 'DELETE', signal });
  }

  async waitForJob(id, { intervalMs = 3000, signal, maxWaitMs = 2 * 60 * 60 * 1000 } = {}) {
    if (intervalMs < 100 || maxWaitMs <= 0) throw new Error('Invalid polling settings');
    const start = Date.now();
    while (Date.now() - start < maxWaitMs) {
      if (signal?.aborted) throw signal.reason ?? new Error('Cancelled');
      const job = await this.getJob(id, { signal });
      if (job.status === 'completed') return job;
      if (['failed', 'cancelled'].includes(job.status)) {
        throw new ToolNetSRTError(job.errorCode || job.status, 409, job);
      }
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          signal?.removeEventListener('abort', abort);
          resolve();
        }, intervalMs);
        const abort = () => {
          clearTimeout(timer);
          signal?.removeEventListener('abort', abort);
          reject(signal.reason ?? new Error('Cancelled'));
        };
        signal?.addEventListener('abort', abort, { once: true });
        if (signal?.aborted) abort();
      });
    }
    throw new ToolNetSRTError('Job polling timed out', 408, { id });
  }
}
