import express, { Request, Response } from 'express';
import { config } from './config';
import { buildValidationReport, KafkaValidationResult } from './kafkaReportService';

const app = express();

app.use(express.urlencoded({ extended: false }));

app.get('/', (_req: Request, res: Response) => {
  res.send(renderPage({
    startOffset: '0'
  }));
});

app.post('/report', async (req: Request, res: Response) => {
  const startOffset = typeof req.body.startOffset === 'string' ? req.body.startOffset : '';

  try {
    const report = await buildValidationReport(startOffset);

    res.send(renderPage({
      startOffset,
      report
    }));
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown error while loading Kafka report.';
    res.status(400).send(renderPage({
      startOffset,
      error: message
    }));
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

      ${error ? `<div class="error">${escapeHtml(error)}</div>` : ''}

      ${showChart
        ? `<div class="summary">
            <h2>Results</h2>
            <p><strong>Prompt:</strong> <textarea></textarea></p>
            <p><strong>Expected result:</strong> <input type="text" name="expectedResult" value="" /></p>
            <p><strong>valid === true:</strong> ${validTrue}</p>
            <p><strong>valid === false:</strong> ${validFalse}</p>
            <p><strong>messages processed:</strong> ${processedMessages}</p>
            <p><strong>offset range:</strong> ${escapeHtml(report!.startOffset)} to ${escapeHtml(report!.latestOffsetExclusive)} (latest exclusive)</p>
          </div>
          <canvas id="reportChart" width="640" height="360"></canvas>
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
        : ''}
    </div>
  </body>
</html>`;
}

function escapeHtml(input: string): string {
  return input
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}
