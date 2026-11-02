#!/usr/bin/env python3
"""Apply ToolNet-SRT v0.2 jobs/docs/MCP upgrade without replacing legacy routes."""
import json
from pathlib import Path

root = Path.cwd()
server = root / 'server/index.mjs'
package = root / 'package.json'
if not server.exists() or not package.exists():
    raise SystemExit('ERROR: Run from /root/Toolnet-SRT repository root')

source = server.read_text(encoding='utf-8')
if 'createExtendedRoutes' in source:
    print('Server patch already applied')
else:
    changes = [
      (
        'import { pipeline } from "node:stream/promises";',
        'import { pipeline } from "node:stream/promises";\n'
        'import { JobManager } from "./jobs.mjs";\n'
        'import { createExtendedRoutes } from "./extended-routes.mjs";',
      ),
      (
        'let busy = false;',
        'let busy = false;\n'
        'const jobManager = new JobManager({ canRun: () => !busy });\n'
        'const extendedRoutes = createExtendedRoutes({\n'
        '  manager: jobManager,\n'
        '  authenticate,\n'
        '  apiKeyPresent: () => Boolean(API_KEY),\n'
        '});',
      ),
      (
        '  if (busy) {',
        '  if (busy || jobManager.isBusy()) {',
      ),
      (
        '    busy = false;\n    res.off("close", onClose);',
        '    busy = false;\n    jobManager.kick();\n    res.off("close", onClose);',
      ),
      (
        '  if (req.method === "GET" && path === "/api/health") {',
        '  if (extendedRoutes.isExtended(path)) {\n'
        '    void extendedRoutes.handle(req, res, path);\n'
        '    return;\n'
        '  }\n\n'
        '  if (req.method === "GET" && path === "/api/health") {',
      ),
    ]
    for old, new in changes:
        if source.count(old) != 1:
            raise SystemExit(f'ERROR: Expected exactly one server anchor: {old[:100]}')
        source = source.replace(old, new, 1)
    server.write_text(source, encoding='utf-8')
    print('Patched server/index.mjs (kept existing /v1/transcribe and health routes)')

pkg = json.loads(package.read_text(encoding='utf-8'))
if pkg.get('name') != 'toolnet-srt':
    raise SystemExit('ERROR: Wrong package')
if pkg.get('version') not in ('0.1.0', '0.2.0'):
    raise SystemExit('ERROR: Unexpected package version; inspect manually')
pkg['version'] = '0.2.0'
pkg.setdefault('exports', {})['./client'] = './sdk/client.mjs'
pkg.setdefault('bin', {})['toolnet-srt-mcp'] = 'mcp/stdio.mjs'
for file in ('sdk', 'mcp'):
    if file not in pkg['files']:
        pkg['files'].append(file)
package.write_text(json.dumps(pkg, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print('Updated package.json -> toolnet-srt@0.2.0, client and MCP stdio exports')

ignore = root / '.gitignore'
content = ignore.read_text(encoding='utf-8') if ignore.exists() else ''
for rule in ('README.md.bak', 'input/', 'output/', '.env', '.env.*', '.toolnet-srt-jobs/'):
    if rule not in content.splitlines():
        content += ('' if content.endswith('\n') else '\n') + rule + '\n'
ignore.write_text(content, encoding='utf-8')
print('Added local test/secret folders to .gitignore (no deletion)')

dockerfile = root / 'Dockerfile'
if not dockerfile.exists():
    raise SystemExit('ERROR: Dockerfile is missing')
docker = dockerfile.read_text(encoding='utf-8')
if 'COPY mcp/ ./mcp/' not in docker:
    anchor = 'COPY server/ ./server/'
    if docker.count(anchor) != 1:
        raise SystemExit('ERROR: Dockerfile expected server copy anchor not found')
    docker = docker.replace(anchor, anchor + '\nCOPY mcp/ ./mcp/\nCOPY sdk/ ./sdk/', 1)
    dockerfile.write_text(docker, encoding='utf-8')
    print('Patched Dockerfile to include MCP and SDK modules')
