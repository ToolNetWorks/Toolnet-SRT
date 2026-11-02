#!/usr/bin/env node
import { createInterface } from 'node:readline';
import { ToolNetSRTClient } from '../sdk/client.mjs';
import { createMcpProtocol } from './protocol.mjs';

const url = process.env.TOOLNET_SRT_BASE_URL || 'https://toolnet-srt.onrender.com';
const key = process.env.TOOLNET_SRT_API_KEY;
if (!key) {
  process.stderr.write('TOOLNET_SRT_API_KEY must be set\n');
  process.exit(1);
}
const protocol = createMcpProtocol(new ToolNetSRTClient({ baseUrl: url, apiKey: key }));
const stdin = createInterface({ input: process.stdin, crlfDelay: Infinity, terminal: false });
for await (const line of stdin) {
  if (!line.trim()) continue;
  let response;
  try { response = await protocol(JSON.parse(line)); }
  catch { response = { jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Parse error' } }; }
  if (response) process.stdout.write(JSON.stringify(response) + '\n');
}
