"""
Centralized Email Alerting Engine — dags/utils/alerting.py

Supports:
  - Rich HTML email via smtplib + MIME (no external dependencies)
  - Gmail SMTP with STARTTLS (port 587)
  - Pre-built Airflow callbacks: on_failure_callback, on_retry_callback
  - Graceful fallback to console log if SMTP is misconfigured
"""

from __future__ import annotations

import logging
import os
import smtplib
import traceback
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

log = logging.getLogger(__name__)

# ── SMTP Config (read from environment / Airflow Variables) ─────────────────
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_SENDER = os.getenv("SMTP_SENDER", SMTP_USER)
ALERT_RECEIVERS_RAW = os.getenv("ALERT_RECEIVERS", "")
ALERT_RECEIVERS: list[str] = [
    addr.strip() for addr in ALERT_RECEIVERS_RAW.split(",") if addr.strip()
]


# ── HTML Template ────────────────────────────────────────────────────────────

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <style>
    body {{
      font-family: 'Segoe UI', Arial, sans-serif;
      background: #0f0f0f;
      color: #e0e0e0;
      margin: 0;
      padding: 20px;
    }}
    .card {{
      max-width: 700px;
      margin: auto;
      background: #1a1a2e;
      border-radius: 12px;
      overflow: hidden;
      box-shadow: 0 4px 24px rgba(0,0,0,0.5);
    }}
    .header {{
      background: {header_color};
      padding: 24px 32px;
      display: flex;
      align-items: center;
      gap: 16px;
    }}
    .header .icon {{ font-size: 32px; }}
    .header h1 {{
      margin: 0;
      font-size: 20px;
      color: #fff;
      font-weight: 700;
      letter-spacing: 0.5px;
    }}
    .header .subtitle {{
      margin: 4px 0 0;
      font-size: 13px;
      color: rgba(255,255,255,0.75);
    }}
    .body {{ padding: 28px 32px; }}
    .meta-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      margin-bottom: 20px;
    }}
    .meta-item {{
      background: rgba(255,255,255,0.05);
      border-radius: 8px;
      padding: 12px 16px;
    }}
    .meta-label {{
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: #888;
      margin-bottom: 4px;
    }}
    .meta-value {{
      font-size: 14px;
      font-weight: 600;
      color: #e0e0e0;
      word-break: break-all;
    }}
    .section-title {{
      font-size: 13px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: #888;
      margin: 20px 0 10px;
    }}
    .traceback {{
      background: #0d1117;
      border: 1px solid #30363d;
      border-radius: 8px;
      padding: 16px;
      font-family: 'Courier New', monospace;
      font-size: 12px;
      line-height: 1.6;
      color: #f85149;
      white-space: pre-wrap;
      overflow-x: auto;
      max-height: 300px;
      overflow-y: auto;
    }}
    .log-link {{
      display: inline-block;
      margin-top: 20px;
      padding: 10px 20px;
      background: linear-gradient(135deg, #667eea, #764ba2);
      color: #fff;
      text-decoration: none;
      border-radius: 8px;
      font-size: 13px;
      font-weight: 600;
      letter-spacing: 0.3px;
    }}
    .footer {{
      border-top: 1px solid rgba(255,255,255,0.08);
      padding: 16px 32px;
      font-size: 11px;
      color: #555;
      text-align: center;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="header">
      <div class="icon">{icon}</div>
      <div>
        <h1>{title}</h1>
        <div class="subtitle">{subtitle}</div>
      </div>
    </div>
    <div class="body">
      <div class="meta-grid">
        <div class="meta-item">
          <div class="meta-label">DAG ID</div>
          <div class="meta-value">{dag_id}</div>
        </div>
        <div class="meta-item">
          <div class="meta-label">Task ID</div>
          <div class="meta-value">{task_id}</div>
        </div>
        <div class="meta-item">
          <div class="meta-label">Execution Date</div>
          <div class="meta-value">{execution_date}</div>
        </div>
        <div class="meta-item">
          <div class="meta-label">Try Number</div>
          <div class="meta-value">{try_number}</div>
        </div>
        <div class="meta-item">
          <div class="meta-label">Run ID</div>
          <div class="meta-value">{run_id}</div>
        </div>
        <div class="meta-item">
          <div class="meta-label">Alert Time (UTC)</div>
          <div class="meta-value">{alert_time}</div>
        </div>
      </div>
      <div class="section-title">Exception / Traceback</div>
      <div class="traceback">{traceback}</div>
      {log_link}
    </div>
    <div class="footer">
      Crypto Coin Pipeline — DataOps Alerting Engine &bull; Auto-generated alert
    </div>
  </div>
</body>
</html>
"""


def _extract_context_metadata(context: dict) -> dict:
    """Extract relevant Airflow task context metadata for the alert email."""
    ti = context.get("task_instance")

    dag_id = ti.dag_id if ti else context.get("dag_id", "unknown")
    task_id = ti.task_id if ti else context.get("task_id", "unknown")
    run_id = ti.run_id if ti else context.get("run_id", "unknown")
    try_number = ti.try_number if ti else context.get("try_number", 1)

    # Prefer logical_date (Airflow 3.x), fallback to execution_date
    execution_date = context.get("logical_date") or context.get("execution_date")
    if execution_date is None and ti:
        execution_date = getattr(ti, "logical_date", None) or getattr(ti, "execution_date", None)

    # Build log URL
    log_url = context.get("task_instance_key_str", "")
    if ti and hasattr(ti, "log_url"):
        log_url = ti.log_url

    # Get exception details
    exception = context.get("exception")
    tb_str = ""
    if exception:
        tb_str = "".join(
            traceback.format_exception(type(exception), exception, exception.__traceback__)
        )
    if not tb_str:
        tb_str = str(exception) if exception else "No traceback available."

    return {
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "try_number": try_number,
        "execution_date": str(execution_date) if execution_date else "N/A",
        "log_url": log_url,
        "traceback": tb_str,
    }


def send_email_alert(
    subject: str,
    html_body: str,
    receivers: list[str] | None = None,
) -> None:
    """
    Send an HTML email via SMTP (Gmail STARTTLS by default).

    Falls back to console logging if SMTP credentials are not configured.
    """
    to_list = receivers or ALERT_RECEIVERS

    if not SMTP_USER or not SMTP_PASSWORD:
        log.warning(
            "[Alerting] SMTP_USER / SMTP_PASSWORD not set. "
            "Falling back to console log.\n\nSubject: %s\n\n%s",
            subject,
            html_body,
        )
        return

    if not to_list:
        log.warning("[Alerting] ALERT_RECEIVERS not configured. Skipping email send.")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_SENDER or SMTP_USER
    msg["To"] = ", ".join(to_list)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, to_list, msg.as_string())
        log.info("[Alerting] Email sent successfully to: %s", to_list)
    except Exception:
        log.exception("[Alerting] Failed to send email alert via SMTP.")


# ── Public callback API ──────────────────────────────────────────────────────


def airflow_task_failure_callback(context: dict) -> None:
    """
    Airflow `on_failure_callback` — sends a rich HTML failure alert email.

    Usage in DAG default_args:
        default_args = {"on_failure_callback": airflow_task_failure_callback}
    """
    meta = _extract_context_metadata(context)
    alert_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    log_link_html = ""
    if meta["log_url"]:
        log_link_html = (
            f'<a class="log-link" href="{meta["log_url"]}" target="_blank">'
            "🔗 View Task Logs in Airflow UI</a>"
        )

    html_body = _HTML_TEMPLATE.format(
        header_color="linear-gradient(135deg, #c0392b, #8e1a0e)",
        icon="🔴",
        title="Pipeline Task Failed",
        subtitle=f"DAG '{meta['dag_id']}' › Task '{meta['task_id']}' has FAILED",
        dag_id=meta["dag_id"],
        task_id=meta["task_id"],
        execution_date=meta["execution_date"],
        try_number=meta["try_number"],
        run_id=meta["run_id"],
        alert_time=alert_time,
        traceback=meta["traceback"],
        log_link=log_link_html,
    )

    subject = (
        f"🔴 [AIRFLOW FAILURE] {meta['dag_id']} › {meta['task_id']} "
        f"(Try #{meta['try_number']}) — {meta['execution_date']}"
    )

    log.error(
        "[Alerting] Task failure detected — DAG: %s, Task: %s, Try: %s",
        meta["dag_id"],
        meta["task_id"],
        meta["try_number"],
    )
    send_email_alert(subject=subject, html_body=html_body)


def airflow_task_retry_callback(context: dict) -> None:
    """
    Airflow `on_retry_callback` — sends a softer HTML warning email on retry.

    Usage in DAG default_args:
        default_args = {"on_retry_callback": airflow_task_retry_callback}
    """
    meta = _extract_context_metadata(context)
    alert_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    log_link_html = ""
    if meta["log_url"]:
        log_link_html = (
            f'<a class="log-link" href="{meta["log_url"]}" target="_blank">'
            "🔗 View Task Logs in Airflow UI</a>"
        )

    html_body = _HTML_TEMPLATE.format(
        header_color="linear-gradient(135deg, #d68910, #9a6a00)",
        icon="🟡",
        title="Pipeline Task Retrying",
        subtitle=f"DAG '{meta['dag_id']}' › Task '{meta['task_id']}' is being retried",
        dag_id=meta["dag_id"],
        task_id=meta["task_id"],
        execution_date=meta["execution_date"],
        try_number=meta["try_number"],
        run_id=meta["run_id"],
        alert_time=alert_time,
        traceback=meta["traceback"],
        log_link=log_link_html,
    )

    subject = (
        f"🟡 [AIRFLOW RETRY] {meta['dag_id']} › {meta['task_id']} "
        f"(Try #{meta['try_number']}) — {meta['execution_date']}"
    )

    log.warning(
        "[Alerting] Task retry triggered — DAG: %s, Task: %s, Try: %s",
        meta["dag_id"],
        meta["task_id"],
        meta["try_number"],
    )
    send_email_alert(subject=subject, html_body=html_body)
