document.addEventListener('DOMContentLoaded', () => {
  const statTotalTx = document.getElementById('stat-total-tx');
  const statTotalVol = document.getElementById('stat-total-vol');
  const statHvCount = document.getElementById('stat-hv-count');
  const statRsCount = document.getElementById('stat-rs-count');
  const statAlertsCount = document.getElementById('stat-alerts-count');
  const txTbody = document.getElementById('transactions-tbody');
  const alertsContainer = document.getElementById('alerts-container');
  const btnRefresh = document.getElementById('btn-manual-refresh');
  const toastContainer = document.getElementById('toast-container');

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
      await Promise.all([fetchStats(), fetchTransactions(), fetchAlerts()]);
    } catch (err) {
      console.error('Error fetching dashboard updates:', err);
    }
  }

  async function fetchStats() {
    const res = await fetch('/api/stats');
    if (!res.ok) return;
    const data = await res.json();
    
    statTotalTx.textContent = Number(data.total_transactions || 0).toLocaleString();
    statTotalVol.textContent = '₹' + Number(data.total_volume_inr || 0).toLocaleString('en-IN', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
    statHvCount.textContent = Number(data.high_value_count || 0).toLocaleString();
    statRsCount.textContent = Number(data.rapid_spending_count || 0).toLocaleString();
    statAlertsCount.textContent = Number(data.total_alerts || 0).toLocaleString();
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
          <td><code style="color: var(--accent-cyan); font-family: 'JetBrains Mono', monospace;">${escapeHtml(tx.transaction_id)}</code></td>
          <td><strong style="color: #cbd5e1;">${escapeHtml(tx.account_id)}</strong></td>
          <td style="font-weight: 600;">₹${formattedAmount}</td>
          <td style="color: var(--text-muted);">${escapeHtml(tx.merchant || 'N/A')}</td>
          <td><span class="badge ${statusClass}">${escapeHtml(tx.status)}</span></td>
          <td style="color: var(--text-dim); font-size: 0.75rem;">${timeStr}</td>
        </tr>
      `;
    }).join('');
  }

  async function fetchAlerts() {
    const res = await fetch('/api/alerts?limit=25');
    if (!res.ok) return;
    const list = await res.json();

    if (!list || list.length === 0) {
      alertsContainer.innerHTML = '<div style="text-align: center; color: var(--text-dim); padding: 2rem;">No security or fraud alerts recorded yet.</div>';
      return;
    }

    alertsContainer.innerHTML = list.map(alert => {
      const iconInfo = getAlertIconInfo(alert.alert_type);
      const timeStr = formatTimestamp(alert.created_at);

      return `
        <div class="alert-item">
          <div class="alert-icon-box ${iconInfo.boxClass}">${iconInfo.icon}</div>
          <div class="alert-body">
            <div class="alert-top">
              <span class="alert-title">${escapeHtml(alert.alert_type)}</span>
              <span class="alert-time">${timeStr}</span>
            </div>
            <div class="alert-desc">${escapeHtml(alert.description)}</div>
          </div>
        </div>
      `;
    }).join('');
  }

  // ── Dispatch Transactions ──────────────────────────────
  async function sendTransaction(payload) {
    try {
      showToast('Publishing event to Kafka...', 'info');
      const res = await fetch('/api/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const data = await res.json();

      if (res.ok && data.status === 'SUCCESS') {
        showToast(data.message || 'Transaction published to Kafka!', 'success');
        // Immediate fetch after a brief delay to allow consumer to persist
        setTimeout(fetchDashboardData, 600);
      } else {
        showToast('Error: ' + (data.error || 'Failed to dispatch transaction'), 'error');
      }
    } catch (err) {
      showToast('Network error while publishing: ' + err.message, 'error');
    }
  }

  // ── Event Handlers ─────────────────────────────────────
  btnSubmitTx.addEventListener('click', () => {
    const account = inputAccount.value.trim() || 'ACC_DEMO';
    const amount = parseFloat(inputAmount.value) || 1500.00;
    const merchant = inputMerchant.value.trim() || 'Demo Store';

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
    showToast('Dashboard data refreshed', 'info');
  });

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

  function getAlertIconInfo(type) {
    if (!type) return { icon: '⚠️', boxClass: 'alert-icon-rs' };
    const t = type.toUpperCase();
    if (t.includes('HIGH_VALUE')) return { icon: '🚨', boxClass: 'alert-icon-hv' };
    if (t.includes('RAPID_SPENDING')) return { icon: '⚡', boxClass: 'alert-icon-rs' };
    if (t.includes('COLLISION')) return { icon: '🛡️', boxClass: 'alert-icon-col' };
    return { icon: '⚠️', boxClass: 'alert-icon-rs' };
  }

  function formatTimestamp(ts) {
    if (!ts) return '';
    try {
      const d = new Date(ts);
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) + ' ' + d.toLocaleDateString();
    } catch {
      return ts;
    }
  }

  function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = 'toast';
    let icon = 'ℹ️';
    if (type === 'success') icon = '✅';
    if (type === 'error') icon = '❌';

    toast.innerHTML = `<span>${icon}</span> <span>${escapeHtml(message)}</span>`;
    toastContainer.appendChild(toast);

    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateX(100%)';
      toast.style.transition = 'all 0.3s ease';
      setTimeout(() => toast.remove(), 300);
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

  // ── Initial Load & Auto-Refresh ───────────────────────
  fetchDashboardData();
  setInterval(fetchDashboardData, 2500);
});
