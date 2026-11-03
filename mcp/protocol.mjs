const SUPPORTED_PROTOCOLS = new Set(['2025-03-26', '2025-06-18', '2025-11-25']);

export const MCP_TOOLS = [
  {
    name: 'submit_transcription_url',
    description: 'Start a SRT transcription job from a permitted HTTPS media URL. The server administrator must allowlist the media hostname.',
    inputSchema: { type: 'object', properties: {
      url: { type: 'string', description: 'HTTPS URL of audio/video on an allowed hostname' },
      idempotency_key: { type: 'string', minLength: 1, maxLength: 255, description: 'Stable retry key; required in durable mode.' },
    }, required: ['url'], additionalProperties: false },
  },
  {
    name: 'get_transcription_job',
    description: 'Get transcription status, progress percentage and stage from a job ID.',
    inputSchema: { type: 'object', properties: { job_id: { type: 'string' } }, required: ['job_id'], additionalProperties: false },
  },
  {
    name: 'get_transcription_srt',
    description: 'Read completed SRT subtitles. Output is capped at 20,000 characters per MCP call.',
    inputSchema: { type: 'object', properties: { job_id: { type: 'string' } }, required: ['job_id'], additionalProperties: false },
  },
  {
    name: 'cancel_transcription_job',
    description: 'Cancel a queued or running transcription job.',
    inputSchema: { type: 'object', properties: { job_id: { type: 'string' } }, required: ['job_id'], additionalProperties: false },
  },
];

function error(id, code, message) {
  return { jsonrpc: '2.0', id: id ?? null, error: { code, message } };
}

function result(id, data) {
  return { jsonrpc: '2.0', id, result: data };
}

function toolResult(value, isError = false) {
  return { content: [{ type: 'text', text: typeof value === 'string' ? value : JSON.stringify(value) }], isError };
}

export function createMcpProtocol(service) {
  return async (request) => {
    if (!request || request.jsonrpc !== '2.0' || typeof request.method !== 'string') {
      return error(null, -32600, 'Invalid Request');
    }
    const { method, params = {}, id } = request;
    if (id === undefined) return null; // JSON-RPC notification
    if (method === 'initialize') {
      const version = SUPPORTED_PROTOCOLS.has(params.protocolVersion) ? params.protocolVersion : '2025-11-25';
      return result(id, {
        protocolVersion: version,
        capabilities: { tools: { listChanged: false } },
        serverInfo: { name: 'toolnet-srt', version: '0.2.0' },
      });
    }
    if (method === 'ping') return result(id, {});
    if (method === 'tools/list') return result(id, { tools: MCP_TOOLS });
    if (method !== 'tools/call') return error(id, -32601, 'Method not found');

    try {
      const { name, arguments: args = {} } = params;
      if (!args || typeof args !== 'object' || Array.isArray(args)) {
        return error(id, -32602, 'Invalid tool arguments');
      }
      if (name === 'submit_transcription_url') {
        if (typeof args.url !== 'string') return error(id, -32602, 'url must be a string');
        if (args.idempotency_key !== undefined && typeof args.idempotency_key !== 'string') return error(id, -32602, 'idempotency_key must be a string');
        return result(id, toolResult(await service.createJobFromUrl(args.url, { idempotencyKey: args.idempotency_key })));
      }
      if (name === 'get_transcription_job') {
        if (typeof args.job_id !== 'string') return error(id, -32602, 'job_id must be a string');
        return result(id, toolResult(await service.getJob(args.job_id)));
      }
      if (name === 'get_transcription_srt') {
        if (typeof args.job_id !== 'string') return error(id, -32602, 'job_id must be a string');
        const content = await service.getResult(args.job_id);
        const text = Buffer.isBuffer(content) ? content.toString('utf8') : String(content);
        return result(id, toolResult({ srt: text.slice(0, 20000), truncated: text.length > 20000 }));
      }
      if (name === 'cancel_transcription_job') {
        if (typeof args.job_id !== 'string') return error(id, -32602, 'job_id must be a string');
        return result(id, toolResult(await service.cancelJob(args.job_id)));
      }
      return error(id, -32602, 'Unknown tool');
    } catch (e) {
      const code = typeof e?.code === 'string' && /^[A-Z][A-Z0-9_]{0,79}$/.test(e.code) ? e.code :
        typeof e?.details?.error === 'string' && /^[A-Z][A-Z0-9_]{0,79}$/.test(e.details.error) ? e.details.error : 'TOOL_ERROR';
      return result(id, toolResult({ error: code, message: code }, true));
    }
  };
}
