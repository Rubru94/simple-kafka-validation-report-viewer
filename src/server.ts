import express, { Request, Response } from "express";
import { config } from "./config";
import {
  buildValidationReport,
  KafkaValidationResult,
} from "./kafkaReportService";

const app = express();

app.use(express.urlencoded({ extended: false }));

app.get("/", (_req: Request, res: Response) => {
  res.send(
    renderPage({
      startOffset: "0",
    }),
  );
});

app.post("/report", async (req: Request, res: Response) => {
  const startOffset =
    typeof req.body.startOffset === "string" ? req.body.startOffset : "";

  try {
    const report = await buildValidationReport(startOffset);

    res.send(
      renderPage({
        startOffset,
        report,
      }),
    );
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "Unknown error while loading Kafka report.";
    res.status(400).send(
      renderPage({
        startOffset,
        error: message,
      }),
    );
  }
});

app.listen(config.port, () => {
  // eslint-disable-next-line no-console
  console.log(`validator-reports listening at http://localhost:${config.port}`);
});

type RenderOptions = {
  startOffset: string;
  report?: KafkaValidationResult;
  error?: string;
};

function renderPage(options: RenderOptions): string {
  const { startOffset, report, error } = options;

  const validTrue = report?.validTrue ?? 0;
  const validFalse = report?.validFalse ?? 0;
  const processedMessages = report?.processedMessages ?? 0;
  const showChart = Boolean(report);

  const validMessages = report?.validMessages ?? [];
  const invalidMessages = report?.invalidMessages ?? [];

  const validMessagesJson = JSON.stringify(validMessages);
  const invalidMessagesJson = JSON.stringify(invalidMessages);

  return `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>validator-reports</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
      body {
        font-family: Arial, sans-serif;
        margin: 2rem;
        color: #1f2937;
      }
      .container {
        max-width: 860px;
        margin: 0 auto;
      }
      h1 {
        margin-bottom: 0.75rem;
      }
      form {
        display: flex;
        gap: 0.75rem;
        align-items: flex-end;
        margin: 1.25rem 0;
      }
      label {
        display: flex;
        flex-direction: column;
        font-weight: 600;
        gap: 0.35rem;
      }
      input {
        padding: 0.45rem 0.5rem;
        min-width: 220px;
      }
      button {
        padding: 0.5rem 0.85rem;
        cursor: pointer;
      }
      .summary {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        padding: 0.9rem;
        margin-bottom: 1rem;
      }
      .error {
        color: #b91c1c;
        margin-bottom: 1rem;
      }
      .muted {
        color: #6b7280;
      }
      canvas {
        max-width: 640px;
        max-height: 360px;
        display: flex;
        margin: 0 auto;
      }
      .summary .grid {
        display: grid;
        grid-template-columns: 180px 1fr;
        gap: 0.5rem 1rem;
        align-items: center;
      }
      .summary .label {
        font-weight: 600;
      }
      .summary textarea {
        width: 95%;
        min-height: 60px;
      }
      .summary input {
        width: 95%;
      }
      .filter-controls {
        display: flex;
        gap: 0.75rem;
        align-items: center;
        margin-bottom: 1rem;
      }
      .messages-container {
        max-height: 400px;
        overflow-y: auto;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        background: #fff;
      }
      .message-item {
        padding: 0.75rem;
        border-bottom: 1px solid #e2e8f0;
        font-size: 0.875rem;
      }
      .message-item:last-child {
        border-bottom: none;
      }
      .message-item.valid {
        background: #ecfdf5;
      }
      .message-item.invalid {
        background: #fef2f2;
      }
      .message-item .field {
        margin-bottom: 0.25rem;
      }
      .message-item .field-label {
        font-weight: 600;
        color: #4b5563;
      }
      .message-item .broken-rules {
        color: #dc2626;
        font-weight: 600;
      }
      .hidden {
        display: none;
      }
      .empty {
        padding: 1rem;
        text-align: center;
        color: #6b7280;
      }
    </style>
  </head>
  <body>
    <div class="container">
      <h1>Kafka Validation Report</h1>
      <p class="muted">Topic: <strong>${escapeHtml(config.topicName)}</strong></p>

      <form method="POST" action="/report">
        <label>
          Starting offset
          <input type="text" name="startOffset" value="${escapeHtml(startOffset)}" required />
        </label>
        <button type="submit">Load report</button>
      </form>

      ${error ? `<div class="error">${escapeHtml(error)}</div>` : ""}

      ${
        showChart
          ? `
          <div class="summary">
          <h2>Results</h2>

          <div class="grid">
            <!--
            <span class="label">Prompt:</span>
            <textarea id="promptInput"></textarea>

            <span class="label">Expected result:</span>
            <input type="text" id="expectedResult" value="" />
            -->

            <span class="label">valid === true:</span>
            <span>${validTrue}</span>

            <span class="label">valid === false:</span>
            <span>${validFalse}</span>

            <span class="label">messages processed:</span>
            <span>${processedMessages}</span>

            <span class="label">offset range:</span>
            <span>${escapeHtml(report!.startOffset)} to ${escapeHtml(report!.latestOffsetExclusive)} (latest exclusive)</span>
          </div>
        </div>
        </div>
          <canvas id="reportChart" width="640" height="360"></canvas>

          <div class="filter-controls">
            <label>
              Filter by valid:
              <select id="filterValid">
                <option value="all">All</option>
                <option value="true">valid === true</option>
                <option value="false">valid === false</option>
              </select>
            </label>
          </div>

          <div class="messages-container">
            <div id="messagesList"></div>
          </div>

          <script>
            function escapeHtml(input) {
              return String(input)
                .replaceAll("&", "&amp;")
                .replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;")
                .replaceAll('"', "&quot;")
                .replaceAll("'", "&#39;");
            }

            const validMessages = ${validMessagesJson};
            const invalidMessages = ${invalidMessagesJson};

            function renderMessages(filterValid) {
              const container = document.getElementById('messagesList');
              let messages = [];

              if (filterValid === 'all') {
                messages = [...invalidMessages, ...validMessages];
              } else if (filterValid === 'true') {
                messages = validMessages;
              } else if (filterValid === 'false') {
                messages = invalidMessages;
              }

              if (messages.length === 0) {
                container.innerHTML = '<div class="empty">No messages found</div>';
                return;
              }

              container.innerHTML = messages.map(msg => \`
                <div class="message-item \${msg.valid ? 'valid' : 'invalid'}">
                  <div class="field">
                    <span class="field-label">valid:</span> \${msg.valid}
                  </div>
                  <div class="field">
                    <span class="field-label">id:</span> \${escapeHtml(msg.id ?? '')}
                  </div>
                  <div class="field">
                    <span class="field-label">taskRevisionId:</span> \${escapeHtml(msg.taskRevisionId ?? '')}
                  </div>
                  <div class="field">
                    <span class="field-label">externalId:</span> \${escapeHtml(msg.externalId ?? '')}
                  </div>
                  <div class="field">
                    <span class="field-label">jiraId:</span> \${escapeHtml(msg.jiraId ?? '')}
                  </div>
                  <div class="field">
                    <span class="field-label">statusCode:</span> \${escapeHtml(msg.statusCode ?? '')}
                  </div>
                  <div class="field">
                    <span class="field-label">statusText:</span> \${escapeHtml(msg.statusText ?? '')}
                  </div>
                  <div class="field">
                    <span class="field-label">validationTokens:</span> \${msg.validationTokens}
                  </div>
                  <div class="field">
                    <span class="field-label">validationModel:</span> \${escapeHtml(msg.validationModel ?? '')}
                  </div>
                  <div class="field">
                    <span class="field-label">validationCost:</span> \${msg.validationCost}
                  </div>
                  <div class="field">
                    <span class="field-label">validationDurationTime:</span> \${msg.validationDurationTime}ms
                  </div>
                  \${msg.brokenRules && msg.brokenRules.length > 0 ? \`
                  <div class="field">
                    <span class="field-label broken-rules">brokenRules:</span>
                    <ul style="margin: 0.25rem 0 0 1.25rem; padding: 0;">
                      \${msg.brokenRules.map(rule => \`<li>\${escapeHtml(rule)}</li>\`).join('')}
                    </ul>
                  </div>
                  \` : ''}
                </div>
              \`).join('');
            }

            document.getElementById('filterValid').addEventListener('change', function() {
              renderMessages(this.value);
            });

            renderMessages('all');
          </script>
          <script>
            const ctx = document.getElementById('reportChart');
            new Chart(ctx, {
              type: 'bar',
              data: {
                labels: ['valid = true', 'valid = false'],
                datasets: [{
                  label: 'Message count',
                  data: [${validTrue}, ${validFalse}],
                  backgroundColor: ['#10b981', '#ef4444']
                }]
              },
              options: {
                responsive: true,
                scales: {
                  y: {
                    beginAtZero: true,
                    ticks: {
                      precision: 0
                    }
                  }
                }
              }
            });
          </script>`
          : ""
      }
    </div>
  </body>
</html>`;
}

function escapeHtml(input: string): string {
  return input
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
