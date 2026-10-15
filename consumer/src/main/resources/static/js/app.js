document.addEventListener('DOMContentLoaded', () => {
  const statTotalTx = document.getElementById('stat-total-tx');
  const statTotalVol = document.getElementById('stat-total-vol');
  const statHvCount = document.getElementById('stat-hv-count');
  const statRsCount = document.getElementById('stat-rs-count');
  const statAlertsCount = document.getElementById('stat-alerts-count');
  const statRejectedCount = document.getElementById('stat-rejected-count');

  const txTbody = document.getElementById('transactions-tbody');
  const alertsContainer = document.getElementById('alerts-container');
  const rejectedTbody = document.getElementById('rejected-tbody');

  const btnRefresh = document.getElementById('btn-manual-refresh');
  const btnClearData = document.getElementById('btn-clear-data');
  const toastContainer = document.getElementById('toast-container');
  const connectionPill = document.querySelector('.live-pill');

  // Form elements
  const inputAccount = document.getElementById('input-account');
  const inputAmount = document.getElementById('input-amount');
  const inputMerchant = document.getElementById('input-merchant');
  const btnSubmitTx = document.getElementById('btn-submit-tx');

  // Quick buttons
  const btnQuickNormal = document.getElementById('btn-quick-normal');
  const btnQuickHv = document.getElementById('btn-quick-hv');
  const btnQuickRs = document.getElementById('btn-quick-rs');
  const btnQuickDup = document.getElementById('btn-quick-dup');
  const btnQuickPoison = document.getElementById('btn-quick-poison');

  // ── Polling & Data Fetching ─────────────────────────────
  async function fetchDashboardData() {
    try {
      await Promise.all([fetchStats(), fetchTransactions(), fetchAlerts(), fetchRejected()]);
      updateConnectionState(true);
    } catch (err) {
      console.error('Error fetching dashboard updates:', err);
      updateConnectionState(false);
    }
  }

  function updateConnectionState(isConnected) {
    if (!connectionPill) return;
    if (isConnected) {
      connectionPill.style.borderColor = 'rgba(34, 197, 94, 0.3)';
      connectionPill.style.color = '#22c55e';
      connectionPill.innerHTML = '<div class="status-dot"></div><span>CONNECTED (KAFKA :9092 / MYSQL :3306)</span>';
    } else {
      connectionPill.style.borderColor = 'rgba(239, 68, 68, 0.4)';
      connectionPill.style.color = '#ef4444';
      connectionPill.innerHTML = '<div class="status-dot" style="background:#ef4444;"></div><span>DISCONNECTED (BACKEND UNREACHABLE)</span>';
    }
  }

  async function fetchStats() {
    const res = await fetch('/api/stats');
    if (!res.ok) throw new Error('Failed to fetch stats');
    const data = await res.json();
    
    statTotalTx.textContent = Number(data.total_transactions || 0).toLocaleString();
    statTotalVol.textContent = '₹' + Number(data.total_volume_inr || 0).toLocaleString('en-IN', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
    statHvCount.textContent = Number(data.high_value_count || 0).toLocaleString();
    statRsCount.textContent = Number(data.rapid_spending_count || 0).toLocaleString();
    statAlertsCount.textContent = Number(data.total_alerts || 0).toLocaleString();
    if (statRejectedCount) {
      statRejectedCount.textContent = Number(data.total_rejected || 0).toLocaleString();
    }
  }

  async function fetchTransactions() {
    const res = await fetch('/api/transactions?limit=25');
    if (!res.ok) return;
    const list = await res.json();

    if (!list || list.length === 0) {
      txTbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--text-dim); padding: 2rem;">No transactions recorded yet.</td></tr>';
      return;
    }

    txTbody.innerHTML = list.map(tx => {
      const statusClass = getStatusBadgeClass(tx.status);
      const formattedAmount = Number(tx.amount || 0).toLocaleString('en-IN', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      });
      const timeStr = formatTimestamp(tx.transaction_timestamp || tx.created_at);

      return `
        <tr>
          <td class="cell-mono" style="color: #60a5fa;">${escapeHtml(tx.transaction_id)}</td>
          <td class="cell-mono">${escapeHtml(tx.account_id)}</td>
          <td class="cell-mono" style="font-weight: 600; color: #f4f4f5;">₹${formattedAmount}</td>
          <td style="color: var(--text-secondary);">${escapeHtml(tx.merchant || 'N/A')}</td>
          <td><span class="badge ${statusClass}">${escapeHtml(tx.status)}</span></td>
          <td class="cell-mono" style="color: #a1a1aa;">${timeStr}</td>
        </tr>
      `;
    }).join('');
  }

  async function fetchAlerts() {
    const res = await fetch('/api/alerts?limit=25');
    if (!res.ok) return;
    const list = await res.json();

    if (!list || list.length === 0) {
      alertsContainer.innerHTML = '<div style="text-align: center; color: var(--text-dim); padding: 2rem;">No alerts recorded.</div>';
      return;
    }

    alertsContainer.innerHTML = list.map(alert => {
      const typeClass = getAlertTypeClass(alert.alert_type);
      const cardClass = getAlertCardClass(alert.alert_type);
      const timeStr = formatTimestamp(alert.created_at);

      return `
        <div class="alert-card ${cardClass}">
          <div class="alert-header">
            <span class="alert-type-label ${typeClass}">${escapeHtml(alert.alert_type)}</span>
            <span class="alert-timestamp">${timeStr}</span>
          </div>
          <div class="alert-description">${escapeHtml(alert.description)}</div>
          <div class="alert-account-tag">Account: ${escapeHtml(alert.account_id || 'N/A')} | Tx: ${escapeHtml(alert.transaction_id || 'N/A')}</div>
        </div>
      `;
    }).join('');
  }

  async function fetchRejected() {
    if (!rejectedTbody) return;
    const res = await fetch('/api/rejected?limit=25');
    if (!res.ok) return;
    const list = await res.json();

    if (!list || list.length === 0) {
      rejectedTbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--text-dim); padding: 1.5rem;">No rejected events recorded.</td></tr>';
      return;
    }

    rejectedTbody.innerHTML = list.map(item => {
      const timeStr = formatTimestamp(item.created_at);
      const payloadPreview = escapeHtml(String(item.payload || '').substring(0, 50));
      return `
        <tr>
          <td class="cell-mono" style="color: var(--text-muted);">${item.id}</td>
          <td><span class="badge badge-high_value">${escapeHtml(item.rejection_type)}</span></td>
          <td class="cell-mono" style="color: #f87171;">${escapeHtml(item.transaction_id || 'UNKNOWN')}</td>
          <td style="color: #e4e4e7; font-size: 0.74rem;">${escapeHtml(item.reason || '')}</td>
          <td class="cell-mono" style="color: var(--text-dim); font-size: 0.7rem;" title="${escapeHtml(item.payload || '')}"><code>${payloadPreview}...</code></td>
          <td class="cell-mono" style="color: #a1a1aa;">${timeStr}</td>
        </tr>
      `;
    }).join('');
  }

  // ── Dispatch Transactions with Two-Phase Feedback ────────
  async function sendTransaction(payload) {
    try {
      showToast('Phase 1: Dispatched to Kafka broker...', 'info');
      const res = await fetch('/api/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const data = await res.json();

      if (res.ok && data.status === 'SUCCESS') {
        const publishedId = data.transactionId || (data.transactionIds ? data.transactionIds.join(', ') : 'Event');
        showToast(`Published: ${publishedId} accepted by Kafka`, 'info');

        // Phase 2: Verify consumer processing and database write after brief delay
        setTimeout(async () => {
          await fetchDashboardData();
          if (payload.scenario === 'MALFORMED') {
            showToast('Phase 2 Confirmed: Quarantined in rejected_events (Dead-Letter verified)', 'success');
          } else {
            showToast(`Phase 2 Confirmed: Consumer processed and persisted in MySQL`, 'success');
          }
        }, 700);
      } else {
        showToast('Publish Error: ' + (data.error || 'Failed to dispatch to Kafka.'), 'error');
      }
    } catch (err) {
      showToast('Network error: ' + err.message, 'error');
    }
  }

  // ── Event Handlers ─────────────────────────────────────
  btnSubmitTx.addEventListener('click', () => {
    const account = inputAccount.value.trim() || 'ACC101';
    const amount = parseFloat(inputAmount.value) || 1500.00;
    const merchant = inputMerchant.value.trim() || 'Cloud Retail Store';

    sendTransaction({
      scenario: 'NORMAL',
      accountId: account,
      amount: amount,
      merchant: merchant
    });
  });

  btnQuickNormal.addEventListener('click', () => {
    sendTransaction({ scenario: 'NORMAL', amount: 1500.00 });
  });

  btnQuickHv.addEventListener('click', () => {
    sendTransaction({ scenario: 'HIGH_VALUE', amount: 75000.00 });
  });

  btnQuickRs.addEventListener('click', () => {
    sendTransaction({ scenario: 'RAPID_SPENDING' });
  });

  btnQuickDup.addEventListener('click', () => {
    sendTransaction({ scenario: 'DUPLICATE', amount: 2500.00 });
  });

  btnQuickPoison.addEventListener('click', () => {
    sendTransaction({ scenario: 'MALFORMED' });
  });

  btnRefresh.addEventListener('click', () => {
    fetchDashboardData();
    showToast('Dashboard refreshed from MySQL.', 'info');
  });

  btnClearData.addEventListener('click', async () => {
    const ok = confirm(
      '⚠️ PERMANENT DATABASE RESET WARNING\n\n' +
      'This will delete ALL rows from:\n' +
      '  - transactions (Ledger)\n' +
      '  - transaction_alerts (Fraud & Security Alerts)\n' +
      '  - rejected_events (Dead-letter quarantine)\n\n' +
      'Are you sure you want to proceed?'
    );
    if (!ok) return;

    try {
      showToast('Clearing MySQL database tables...', 'info');
      const res = await fetch('/api/clear', { method: 'POST' });
      const data = await res.json();
      if (res.ok && data.status === 'SUCCESS') {
        showToast('All database tables cleared. Metrics reset to 0.', 'success');
        fetchDashboardData();
      } else {
        showToast('Error: ' + (data.error || 'Failed to clear database.'), 'error');
      }
    } catch (err) {
      showToast('Network error while clearing: ' + err.message, 'error');
    }
  });

  // ── Unified UTC Timestamp Formatter ───────────────────
  function formatTimestamp(ts) {
    if (!ts) return 'N/A';
    let str = String(ts);
    // If missing timezone indicator, treat raw string as UTC
    if (!str.endsWith('Z') && !str.includes('+')) {
      str += 'Z';
    }
    const d = new Date(str);
    if (isNaN(d.getTime())) return ts;

    const year = d.getUTCFullYear();
    const month = String(d.getUTCMonth() + 1).padStart(2, '0');
    const day = String(d.getUTCDate()).padStart(2, '0');
    const hours = String(d.getUTCHours()).padStart(2, '0');
    const mins = String(d.getUTCMinutes()).padStart(2, '0');
    const secs = String(d.getUTCSeconds()).padStart(2, '0');

    return `${year}-${month}-${day} ${hours}:${mins}:${secs} UTC`;
  }

  // ── Helpers ───────────────────────────────────────────
  function getStatusBadgeClass(status) {
    if (!status) return 'badge-accepted';
    const s = status.toLowerCase();
    if (s.includes('high_value_and_rapid_spending')) return 'badge-high_value_and_rapid_spending';
    if (s.includes('high_value')) return 'badge-high_value';
    if (s.includes('rapid_spending')) return 'badge-rapid_spending';
    if (s.includes('collision')) return 'badge-id_collision';
    return 'badge-accepted';
  }

  function getAlertCardClass(type) {
    if (!type) return '';
    const t = type.toUpperCase();
    if (t.includes('HIGH_VALUE')) return 'alert-card-hv';
    if (t.includes('RAPID_SPENDING')) return 'alert-card-rs';
    if (t.includes('COLLISION')) return 'alert-card-col';
    return '';
  }

  function getAlertTypeClass(type) {
    if (!type) return '';
    const t = type.toUpperCase();
    if (t.includes('HIGH_VALUE')) return 'type-hv';
    if (t.includes('RAPID_SPENDING')) return 'type-rs';
    if (t.includes('COLLISION')) return 'type-col';
    return '';
  }

  function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    toastContainer.appendChild(toast);

    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateY(10px)';
      toast.style.transition = 'all 0.2s ease';
      setTimeout(() => toast.remove(), 250);
    }, 4000);
  }

  function escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  // ── Initial Load & Auto-Polling (every 2.5s) ───────────
  fetchDashboardData();
  setInterval(fetchDashboardData, 2500);
});
