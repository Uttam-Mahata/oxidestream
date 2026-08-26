import { writeFileSync, mkdirSync } from 'fs';
import { join } from 'path';

const OUT_DIR = join(import.meta.dirname, 'output');
mkdirSync(OUT_DIR, { recursive: true });

let idCounter = 0;
function uid() { return `el_${++idCounter}`; }

function rect(x, y, w, h, opts = {}) {
  return {
    id: uid(), type: 'rectangle', x, y, width: w, height: h,
    angle: 0, strokeColor: opts.strokeColor || '#1e1e1e',
    backgroundColor: opts.bg || 'transparent', fillStyle: 'solid',
    strokeWidth: opts.strokeWidth || 2, roughness: 1, opacity: 100,
    groupIds: opts.groupIds || [], frameId: null, roundness: { type: 3 },
    boundElements: opts.boundElements || [], updated: Date.now(),
    link: null, locked: false,
  };
}

function roundedRect(x, y, w, h, opts = {}) {
  return { ...rect(x, y, w, h, opts), roundness: { type: 3 } };
}

function ellipse(x, y, w, h, opts = {}) {
  return {
    id: uid(), type: 'ellipse', x, y, width: w, height: h,
    angle: 0, strokeColor: opts.strokeColor || '#1e1e1e',
    backgroundColor: opts.bg || 'transparent', fillStyle: 'solid',
    strokeWidth: opts.strokeWidth || 2, roughness: 1, opacity: 100,
    groupIds: opts.groupIds || [], frameId: null, roundness: { type: 2 },
    boundElements: opts.boundElements || [], updated: Date.now(),
    link: null, locked: false,
  };
}

function text(x, y, content, opts = {}) {
  const lines = content.split('\n');
  const lineH = (opts.fontSize || 20) * 1.25;
  const w = Math.max(...lines.map(l => l.length * (opts.fontSize || 20) * 0.6));
  const h = lines.length * lineH;
  return {
    id: uid(), type: 'text', x, y, width: w, height: h,
    angle: 0, strokeColor: opts.color || '#1e1e1e',
    backgroundColor: 'transparent', fillStyle: 'solid',
    strokeWidth: 1, roughness: 1, opacity: 100,
    groupIds: opts.groupIds || [], frameId: null, roundness: null,
    boundElements: [], updated: Date.now(), link: null, locked: false,
    text: content, fontSize: opts.fontSize || 20,
    fontFamily: opts.fontFamily || 5, textAlign: opts.textAlign || 'center',
    verticalAlign: 'middle', containerId: opts.containerId || null,
    originalText: content, autoResize: true, lineHeight: 1.25,
  };
}

function arrow(x1, y1, x2, y2, opts = {}) {
  const dx = x2 - x1, dy = y2 - y1;
  return {
    id: uid(), type: 'arrow', x: x1, y: y1, width: Math.abs(dx), height: Math.abs(dy),
    angle: 0, strokeColor: opts.strokeColor || '#1e1e1e',
    backgroundColor: 'transparent', fillStyle: 'solid',
    strokeWidth: opts.strokeWidth || 2, roughness: 1, opacity: 100,
    groupIds: [], frameId: null, roundness: { type: 2 },
    boundElements: [], updated: Date.now(), link: null, locked: false,
    points: [[0, 0], [dx, dy]], lastCommittedPoint: null,
    startBinding: opts.startBinding || null,
    endBinding: opts.endBinding || null,
    startArrowhead: null, endArrowhead: opts.endArrowhead || 'arrow',
  };
}

function frame(x, y, w, h, label) {
  return {
    id: uid(), type: 'frame', x, y, width: w, height: h,
    angle: 0, strokeColor: '#868e96', backgroundColor: 'transparent',
    fillStyle: 'solid', strokeWidth: 1, roughness: 0, opacity: 100,
    groupIds: [], frameId: null, roundness: null, boundElements: [],
    updated: Date.now(), link: null, locked: false, name: label || '',
  };
}

function makeExcalidraw(elements) {
  return {
    type: 'excalidraw', version: 2, source: 'https://excalidraw.com',
    elements, appState: {
      gridSize: null, viewBackgroundColor: '#ffffff',
    }, files: {},
  };
}

// ═══════════════════════════════════════════════════════════════
// DIAGRAM 1: Level 0 - System Overview
// ═══════════════════════════════════════════════════════════════
function createLevel0() {
  const elements = [];

  // Title
  elements.push(text(280, 20, 'OxideStream — System Overview', { fontSize: 28, color: '#1e1e1e' }));

  // Client
  const clientR = rect(50, 120, 160, 60, { bg: '#e3fafc', strokeColor: '#15aabf' });
  elements.push(clientR);
  elements.push(text(55, 130, 'Client\nREST API', { fontSize: 16, containerId: clientR.id }));

  // Control Plane box
  const cpFrame = frame(280, 100, 420, 300, 'Go Control Plane');
  elements.push(cpFrame);

  const cpBg = rect(290, 130, 400, 260, { bg: '#fff9db', strokeColor: '#fcc419', strokeWidth: 2 });
  elements.push(cpBg);
  elements.push(text(380, 140, 'Go Control Plane (Master)', { fontSize: 20, color: '#e67700' }));

  // Sub-components inside CP
  const sched = rect(310, 185, 170, 50, { bg: '#d3f9d8', strokeColor: '#40c057' });
  elements.push(sched);
  elements.push(text(315, 192, 'DAG Scheduler\nFair Scheduler', { fontSize: 13 }));

  const raft = rect(500, 185, 160, 50, { bg: '#d0bfff', strokeColor: '#7950f2' });
  elements.push(raft);
  elements.push(text(505, 192, 'Raft Metadata\nStore', { fontSize: 13 }));

  const rest = rect(310, 260, 170, 50, { bg: '#c5f6fa', strokeColor: '#15aabf' });
  elements.push(rest);
  elements.push(text(315, 267, 'REST API\nHTTP/JSON', { fontSize: 13 }));

  const tracker = rect(500, 260, 160, 50, { bg: '#ffe3e3', strokeColor: '#fa5252' });
  elements.push(tracker);
  elements.push(text(505, 267, 'Worker Tracker\nHeartbeats', { fontSize: 13 }));

  const metrics = rect(310, 335, 170, 50, { bg: '#fff3bf', strokeColor: '#fab005' });
  elements.push(metrics);
  elements.push(text(315, 342, 'Prometheus\nMetrics', { fontSize: 13 }));

  const operator = rect(500, 335, 160, 50, { bg: '#eebefa', strokeColor: '#cc5de8' });
  elements.push(operator);
  elements.push(text(505, 342, 'K8s Operator\nSimulator', { fontSize: 13 }));

  // gRPC label
  elements.push(text(430, 415, 'gRPC (SubmitTask / CancelTask / UpdateTaskStatus)', { fontSize: 12, color: '#868e96' }));

  // Workers
  const w1Frame = frame(760, 100, 280, 300, 'Rust Workers');
  elements.push(w1Frame);

  const w1Bg = rect(770, 130, 260, 260, { bg: '#fff4e6', strokeColor: '#fd7e14', strokeWidth: 2 });
  elements.push(w1Bg);
  elements.push(text(830, 140, 'Rust Worker Nodes', { fontSize: 20, color: '#d9480f' }));

  const df1 = rect(790, 185, 220, 45, { bg: '#d3f9d8', strokeColor: '#40c057' });
  elements.push(df1);
  elements.push(text(795, 190, 'DataFusion SQL Engine', { fontSize: 13 }));

  const flight1 = rect(790, 245, 220, 45, { bg: '#c5f6fa', strokeColor: '#15aabf' });
  elements.push(flight1);
  elements.push(text(795, 250, 'Arrow Flight Server (ESS)', { fontSize: 13 }));

  const codegen1 = rect(790, 305, 220, 45, { bg: '#d0bfff', strokeColor: '#7950f2' });
  elements.push(codegen1);
  elements.push(text(795, 310, 'CodeGen + Connectors', { fontSize: 13 }));

  // Arrows
  elements.push(arrow(210, 150, 290, 150, { strokeColor: '#15aabf' }));
  elements.push(arrow(690, 260, 770, 260, { strokeColor: '#e67700', strokeWidth: 3 }));

  // Shuffle label
  elements.push(text(700, 415, 'Arrow Flight (Shuffle Transport)', { fontSize: 12, color: '#868e96' }));

  // Worker 2 (smaller)
  const w2Bg = rect(1080, 185, 180, 200, { bg: '#fff4e6', strokeColor: '#fd7e14', strokeWidth: 1 });
  elements.push(w2Bg);
  elements.push(text(1100, 195, 'Worker 2', { fontSize: 16, color: '#d9480f' }));
  const df2 = rect(1095, 230, 150, 35, { bg: '#d3f9d8', strokeColor: '#40c057' });
  elements.push(df2);
  elements.push(text(1100, 235, 'DataFusion', { fontSize: 11 }));
  const flight2 = rect(1095, 280, 150, 35, { bg: '#c5f6fa', strokeColor: '#15aabf' });
  elements.push(flight2);
  elements.push(text(1100, 285, 'Arrow Flight', { fontSize: 11 }));

  // Arrow between workers
  elements.push(arrow(1030, 260, 1080, 260, { strokeColor: '#15aabf', strokeColor: '#868e96' }));
  elements.push(text(1035, 235, 'Shuffle', { fontSize: 10, color: '#868e96' }));

  return makeExcalidraw(elements);
}

// ═══════════════════════════════════════════════════════════════
// DIAGRAM 2: Level 1 - Control Plane Internals
// ═══════════════════════════════════════════════════════════════
function createLevel1ControlPlane() {
  const elements = [];
  elements.push(text(200, 15, 'Control Plane Internals', { fontSize: 28 }));

  // REST API Gateway
  const gateway = rect(50, 80, 200, 400, { bg: '#e3fafc', strokeColor: '#15aabf', strokeWidth: 2 });
  elements.push(gateway);
  elements.push(text(60, 90, 'REST API Gateway', { fontSize: 18, color: '#0c8599' }));

  const endpoints = ['/submit', '/status', '/jobs', '/workers', '/metrics', '/health'];
  endpoints.forEach((ep, i) => {
    const r = rect(65, 140 + i * 50, 170, 35, { bg: '#ffffff', strokeColor: '#15aabf' });
    elements.push(r);
    elements.push(text(75, 145 + i * 50, ep, { fontSize: 14, color: '#15aabf' }));
  });

  // Auth middleware
  const auth = rect(65, 445, 170, 35, { bg: '#ffe3e3', strokeColor: '#fa5252' });
  elements.push(auth);
  elements.push(text(75, 450, 'Auth / CORS / RateLimit', { fontSize: 12, color: '#fa5252' }));

  // Scheduler
  const schedFrame = frame(300, 80, 350, 400, 'Scheduler');
  elements.push(schedFrame);
  const schedBg = rect(310, 110, 330, 370, { bg: '#fff9db', strokeColor: '#fcc419', strokeWidth: 2 });
  elements.push(schedBg);
  elements.push(text(390, 120, 'Scheduler Engine', { fontSize: 20, color: '#e67700' }));

  const dag = rect(330, 165, 290, 45, { bg: '#d3f9d8', strokeColor: '#40c057' });
  elements.push(dag);
  elements.push(text(340, 170, 'DAG Scheduler (Map → Reduce)', { fontSize: 14 }));

  const fair = rect(330, 225, 290, 45, { bg: '#d0bfff', strokeColor: '#7950f2' });
  elements.push(fair);
  elements.push(text(340, 230, 'Fair Scheduler (Job Fairness)', { fontSize: 14 }));

  const stream = rect(330, 285, 290, 45, { bg: '#c5f6fa', strokeColor: '#15aabf' });
  elements.push(stream);
  elements.push(text(340, 290, 'Streaming Scheduler (Micro-batch)', { fontSize: 14 }));

  const dpp = rect(330, 345, 135, 45, { bg: '#eebefa', strokeColor: '#cc5de8' });
  elements.push(dpp);
  elements.push(text(340, 350, 'DPP', { fontSize: 14 }));

  const cbo = rect(485, 345, 135, 45, { bg: '#eebefa', strokeColor: '#cc5de8' });
  elements.push(cbo);
  elements.push(text(495, 350, 'CBO', { fontSize: 14 }));

  const ml = rect(330, 405, 135, 45, { bg: '#fff3bf', strokeColor: '#fab005' });
  elements.push(ml);
  elements.push(text(340, 410, 'ML / Graph', { fontSize: 14 }));

  const spec = rect(485, 405, 135, 45, { bg: '#fff3bf', strokeColor: '#fab005' });
  elements.push(spec);
  elements.push(text(495, 410, 'Speculative', { fontSize: 14 }));

  // Metadata Store
  const metaFrame = frame(700, 80, 280, 250, 'Metadata Store');
  elements.push(metaFrame);
  const metaBg = rect(710, 110, 260, 220, { bg: '#d0bfff', strokeColor: '#7950f2', strokeWidth: 2 });
  elements.push(metaBg);
  elements.push(text(760, 120, 'Raft Metadata Store', { fontSize: 18, color: '#7048e8' }));

  const wr = rect(730, 160, 220, 40, { bg: '#ffffff', strokeColor: '#7950f2' });
  elements.push(wr);
  elements.push(text(740, 165, 'Worker Registry', { fontSize: 14 }));

  const tc = rect(730, 215, 220, 40, { bg: '#ffffff', strokeColor: '#7950f2' });
  elements.push(tc);
  elements.push(text(740, 220, 'Table Catalog (Stats)', { fontSize: 14 }));

  const pi = rect(730, 270, 220, 40, { bg: '#ffffff', strokeColor: '#7950f2' });
  elements.push(pi);
  elements.push(text(740, 275, 'Partition Info', { fontSize: 14 }));

  // Worker Tracker
  const trackerBg = rect(710, 360, 260, 120, { bg: '#ffe3e3', strokeColor: '#fa5252', strokeWidth: 2 });
  elements.push(trackerBg);
  elements.push(text(760, 370, 'Worker Tracker', { fontSize: 18, color: '#c92a2a' }));
  const hb = rect(730, 405, 220, 30, { bg: '#ffffff', strokeColor: '#fa5252' });
  elements.push(hb);
  elements.push(text(740, 408, 'Heartbeat Monitor', { fontSize: 12 }));
  const fd = rect(730, 445, 220, 30, { bg: '#ffffff', strokeColor: '#fa5252' });
  elements.push(fd);
  elements.push(text(740, 448, 'Failure Detection', { fontSize: 12 }));

  // Metrics
  const metricsBg = rect(710, 500, 260, 60, { bg: '#fff3bf', strokeColor: '#fab005', strokeWidth: 2 });
  elements.push(metricsBg);
  elements.push(text(760, 510, 'Prometheus Metrics', { fontSize: 16, color: '#e67700' }));

  // Arrows
  elements.push(arrow(250, 280, 310, 280, { strokeColor: '#e67700' }));
  elements.push(arrow(640, 280, 710, 220, { strokeColor: '#7048e8' }));
  elements.push(arrow(640, 350, 710, 400, { strokeColor: '#c92a2a' }));
  elements.push(arrow(710, 460, 710, 500, { strokeColor: '#e67700' }));

  return makeExcalidraw(elements);
}

// ═══════════════════════════════════════════════════════════════
// DIAGRAM 3: Level 1 - Data Plane Internals
// ═══════════════════════════════════════════════════════════════
function createLevel1DataPlane() {
  const elements = [];
  elements.push(text(250, 15, 'Data Plane — Worker Internals', { fontSize: 28 }));

  // gRPC Client (register/heartbeat)
  const grpcBg = rect(50, 80, 200, 120, { bg: '#e3fafc', strokeColor: '#15aabf', strokeWidth: 2 });
  elements.push(grpcBg);
  elements.push(text(60, 90, 'gRPC Client', { fontSize: 18, color: '#0c8599' }));
  elements.push(text(60, 120, '• RegisterWorker\n• Heartbeat\n• UpdateTaskStatus', { fontSize: 12, textAlign: 'left' }));

  // HTTP Server
  const httpBg = rect(50, 230, 200, 100, { bg: '#fff3bf', strokeColor: '#fab005', strokeWidth: 2 });
  elements.push(httpBg);
  elements.push(text(60, 240, 'HTTP Server (9090)', { fontSize: 16, color: '#e67700' }));
  elements.push(text(60, 270, '• /health\n• /metrics\n• /tasks', { fontSize: 12, textAlign: 'left' }));

  // Executor
  const execFrame = frame(300, 80, 350, 400, 'Executor');
  elements.push(execFrame);
  const execBg = rect(310, 110, 330, 370, { bg: '#d3f9d8', strokeColor: '#40c057', strokeWidth: 2 });
  elements.push(execBg);
  elements.push(text(390, 120, 'SQL Executor', { fontSize: 20, color: '#2b8a3e' }));

  const df = rect(330, 165, 290, 50, { bg: '#ffffff', strokeColor: '#40c057' });
  elements.push(df);
  elements.push(text(340, 172, 'DataFusion SessionContext\nRegister CSV → MemTable', { fontSize: 13 }));

  const sqlExec = rect(330, 230, 290, 50, { bg: '#ffffff', strokeColor: '#40c057' });
  elements.push(sqlExec);
  elements.push(text(340, 237, 'Execute Map/Reduce SQL\nArrow RecordBatch Output', { fontSize: 13 }));

  const shuffle = rect(330, 295, 290, 50, { bg: '#ffffff', strokeColor: '#40c057' });
  elements.push(shuffle);
  elements.push(text(340, 302, 'Hash Partition Output\nWrite .arrow + .index files', { fontSize: 13 }));

  const broadcast = rect(330, 360, 290, 50, { bg: '#ffffff', strokeColor: '#40c057' });
  elements.push(broadcast);
  elements.push(text(340, 367, 'Load Broadcast Tables\nJoin with Partition Data', { fontSize: 13 }));

  const streaming = rect(330, 425, 290, 45, { bg: '#c5f6fa', strokeColor: '#15aabf' });
  elements.push(streaming);
  elements.push(text(340, 430, 'Streaming: Watermark + Checkpoint', { fontSize: 13 }));

  // Arrow Flight Server
  const flightFrame = frame(700, 80, 300, 280, 'Arrow Flight');
  elements.push(flightFrame);
  const flightBg = rect(710, 110, 280, 250, { bg: '#c5f6fa', strokeColor: '#15aabf', strokeWidth: 2 });
  elements.push(flightBg);
  elements.push(text(770, 120, 'Arrow Flight Server', { fontSize: 18, color: '#0c8599' }));

  const ess = rect(730, 165, 240, 45, { bg: '#ffffff', strokeColor: '#15aabf' });
  elements.push(ess);
  elements.push(text(740, 170, 'External Shuffle Service (ESS)', { fontSize: 13 }));

  const pull = rect(730, 225, 240, 45, { bg: '#ffffff', strokeColor: '#15aabf' });
  elements.push(pull);
  elements.push(text(740, 230, 'Pull: Get (index-based seeks)', { fontSize: 13 }));

  const push = rect(730, 285, 240, 45, { bg: '#ffffff', strokeColor: '#15aabf' });
  elements.push(push);
  elements.push(text(740, 290, 'Push: DoPut (to Merger Node)', { fontSize: 13 }));

  // CodeGen
  const codegenBg = rect(710, 380, 280, 100, { bg: '#d0bfff', strokeColor: '#7950f2', strokeWidth: 2 });
  elements.push(codegenBg);
  elements.push(text(760, 390, 'Whole-Stage CodeGen', { fontSize: 16, color: '#7048e8' }));
  elements.push(text(720, 420, 'Compiles AST → tight Arrow\nbuffer loops, bypasses\nper-row dispatch', { fontSize: 12, textAlign: 'left' }));

  // Connectors
  const connBg = rect(710, 500, 280, 70, { bg: '#eebefa', strokeColor: '#cc5de8', strokeWidth: 2 });
  elements.push(connBg);
  elements.push(text(760, 510, 'Connectors', { fontSize: 16, color: '#862e9c' }));
  elements.push(text(720, 535, 'SQLite → Arrow RecordBatch', { fontSize: 12 }));

  // Arrows
  elements.push(arrow(250, 140, 310, 140, { strokeColor: '#0c8599' }));
  elements.push(arrow(250, 280, 310, 300, { strokeColor: '#e67700' }));
  elements.push(arrow(640, 250, 710, 200, { strokeColor: '#0c8599' }));
  elements.push(arrow(640, 400, 710, 420, { strokeColor: '#7048e8' }));

  return makeExcalidraw(elements);
}

// ═══════════════════════════════════════════════════════════════
// DIAGRAM 4: Level 2 - Batch SQL Data Flow
// ═══════════════════════════════════════════════════════════════
function createLevel2DataFlow() {
  const elements = [];
  elements.push(text(200, 15, 'Batch SQL — Data Flow (Map → Shuffle → Reduce)', { fontSize: 24 }));

  // Columns: Client, Master, Worker 1, Worker 2
  const cols = [
    { label: 'Client', x: 50 },
    { label: 'Go Master', x: 280 },
    { label: 'Rust Worker 1', x: 580 },
    { label: 'Rust Worker 2', x: 880 },
  ];

  cols.forEach(c => {
    const bg = rect(c.x, 60, 200, 40, { bg: '#e3fafc', strokeColor: '#15aabf' });
    elements.push(bg);
    elements.push(text(c.x + 10, 67, c.label, { fontSize: 16, color: '#0c8599' }));
  });

  // Timeline
  let y = 140;
  const step = (label, fromX, toX, yPos, opts = {}) => {
    elements.push(text(50, yPos, label, { fontSize: 13, color: '#495057', textAlign: 'left' }));
    if (fromX !== toX) {
      elements.push(arrow(fromX + 200, yPos + 10, toX, yPos + 10, {
        strokeColor: opts.color || '#1e1e1e',
        strokeWidth: opts.width || 2,
      }));
    }
    if (opts.label) {
      elements.push(text((fromX + toX + 200) / 2 - 50, yPos - 15, opts.label, {
        fontSize: 10, color: '#868e96',
      }));
    }
  };

  step('1. POST /submit', 50, 280, y, { label: 'JSON payload' }); y += 45;
  step('2. SubmitTask(MAP)', 280, 580, y, { label: 'gRPC', color: '#40c057' }); y += 45;
  step('3. SubmitTask(MAP)', 280, 880, y, { label: 'gRPC', color: '#40c057' }); y += 45;
  step('4. {job_id, status}', 280, 50, y, { label: 'JSON response' }); y += 50;

  step('5. Read CSV + Execute SQL', 580, 580, y, { color: '#2b8a3e' }); y += 40;
  step('6. Write .arrow + .index', 580, 580, y, { color: '#2b8a3e' }); y += 45;

  step('7. UpdateTaskStatus', 580, 280, y, { label: 'partition metadata', color: '#e67700' }); y += 50;

  step('8. [AQE: coalesce partitions]', 280, 280, y, { color: '#7048e8' }); y += 50;

  step('9. SubmitTask(REDUCE)', 280, 580, y, { label: 'shuffle inputs', color: '#fa5252' }); y += 45;
  step('10. SubmitTask(REDUCE)', 280, 880, y, { label: 'shuffle inputs', color: '#fa5252' }); y += 50;

  // Shuffle arrows between workers
  elements.push(text(580, y, '← Arrow Flight Get →', { fontSize: 12, color: '#15aabf' }));
  elements.push(arrow(780, y + 10, 880, y + 10, { strokeColor: '#15aabf', strokeColor: '#15aabf' }));
  y += 45;

  step('11. Run reduce SQL + write output', 580, 580, y, { color: '#2b8a3e' }); y += 45;
  step('12. UpdateTaskStatus', 580, 280, y, { label: 'COMPLETED', color: '#40c057' }); y += 50;

  step('13. {status: COMPLETED}', 280, 50, y, { label: 'JSON response' });

  return makeExcalidraw(elements);
}

// ═══════════════════════════════════════════════════════════════
// DIAGRAM 5: Level 2 - Communication Protocols
// ═══════════════════════════════════════════════════════════════
function createLevel2Protocols() {
  const elements = [];
  elements.push(text(200, 15, 'Communication Protocols', { fontSize: 28 }));

  // Protocol boxes
  const protocols = [
    {
      name: 'gRPC', subtitle: 'Control ↔ Data Plane', x: 50, y: 80, w: 320, h: 280,
      bg: '#d3f9d8', border: '#40c057',
      items: [
        'ControlPlane Service:',
        '  RegisterWorker(req) → resp',
        '  Heartbeat(req) → resp',
        '  UpdateTaskStatus(req) → resp',
        '',
        'WorkerControl Service:',
        '  SubmitTask(req) → resp',
        '  CancelTask(req) → resp',
        '  PlanQuery(req) → resp',
      ],
    },
    {
      name: 'Arrow Flight', subtitle: 'Worker ↔ Worker (Shuffle)', x: 420, y: 80, w: 320, h: 280,
      bg: '#c5f6fa', border: '#15aabf',
      items: [
        'Pull-based (default):',
        '  Get(Ticket) → RecordBatch stream',
        '  Index-based byte range seeks',
        '',
        'Push-based (Phase 3):',
        '  DoPut → Merger Node',
        '  Consolidated shuffle files',
        '',
        'ESS: Isolated flight thread',
      ],
    },
    {
      name: 'REST HTTP', subtitle: 'External ↔ Control Plane', x: 790, y: 80, w: 320, h: 280,
      bg: '#fff9db', border: '#fcc419',
      items: [
        'Job Management:',
        '  POST /submit, /submit_lr, /submit_pagerank',
        '  POST /submit_streaming',
        '  GET /status, /jobs, /jobs/{id}/tasks',
        '  POST /jobs/{id}/cancel',
        '',
        'Monitoring:',
        '  GET /workers, /queue_depth, /metrics',
        '  GET /health, /metrics/prometheus',
      ],
    },
  ];

  protocols.forEach(p => {
    const bg = rect(p.x, p.y, p.w, p.h, { bg: p.bg, strokeColor: p.border, strokeWidth: 2 });
    elements.push(bg);
    elements.push(text(p.x + 10, p.y + 10, p.name, { fontSize: 20, color: p.border }));
    elements.push(text(p.x + 10, p.y + 35, p.subtitle, { fontSize: 12, color: '#868e96' }));
    elements.push(text(p.x + 15, p.y + 60, p.items.join('\n'), { fontSize: 11, textAlign: 'left' }));
  });

  // Proto definition reference
  elements.push(text(50, 390, 'proto/control.proto — Shared gRPC service definitions (protobuf)', { fontSize: 14, color: '#495057' }));

  // Data format
  const fmtBg = rect(50, 430, 1060, 80, { bg: '#f8f9fa', strokeColor: '#dee2e6', strokeWidth: 1 });
  elements.push(fmtBg);
  elements.push(text(60, 440, 'Data Format: Apache Arrow RecordBatch (IPC)', { fontSize: 16, color: '#1e1e1e' }));
  elements.push(text(60, 465, '• Columnar memory layout  • Zero-copy serialization  • SIMD-friendly  • Cross-language (Go ↔ Rust via Arrow FFI)', { fontSize: 12, color: '#868e96' }));

  return makeExcalidraw(elements);
}

// ═══════════════════════════════════════════════════════════════
// Generate all diagrams
// ═══════════════════════════════════════════════════════════════
const diagrams = [
  { name: 'level0-system-overview', data: createLevel0() },
  { name: 'level1-control-plane', data: createLevel1ControlPlane() },
  { name: 'level1-data-plane', data: createLevel1DataPlane() },
  { name: 'level2-data-flow', data: createLevel2DataFlow() },
  { name: 'level2-protocols', data: createLevel2Protocols() },
];

for (const d of diagrams) {
  const path = join(OUT_DIR, `${d.name}.excalidraw`);
  writeFileSync(path, JSON.stringify(d.data, null, 2));
  console.log(`Generated: ${path}`);
}

console.log(`\n${diagrams.length} diagrams generated in ${OUT_DIR}`);
