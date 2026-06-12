export const chartDefaults = {
  responsive: true,
  maintainAspectRatio: false,
  interaction: {
    mode: "index",
    intersect: false
  },
  layout: {
    padding: {
      top: 8,
      right: 10,
      bottom: 6,
      left: 6
    }
  },
  plugins: {
    legend: {
      labels: {
        usePointStyle: true,
        boxWidth: 10,
        color: "#4b6884",
        padding: 16
      }
    },
    tooltip: {
      backgroundColor: "rgba(14, 44, 74, 0.96)",
      titleColor: "#ffffff",
      bodyColor: "#eff7ff",
      padding: 12,
      cornerRadius: 10,
      displayColors: true
    }
  },
  scales: {
    x: {
      ticks: { color: "#5f7891", padding: 8 },
      grid: { display: false, drawBorder: false },
      border: { display: false }
    },
    y: {
      ticks: { color: "#5f7891", padding: 10 },
      grid: { color: "rgba(31, 95, 157, 0.12)", drawBorder: false },
      border: { display: false }
    }
  }
};

export function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }

  return new Intl.NumberFormat("gl-ES").format(value);
}

export function formatCurrency(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }

  return new Intl.NumberFormat("gl-ES", {
    style: "currency",
    currency: "EUR",
    maximumFractionDigits: 0
  }).format(value);
}

export function formatPercent(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }

  return new Intl.NumberFormat("gl-ES", {
    style: "percent",
    minimumFractionDigits: 1,
    maximumFractionDigits: 1
  }).format(value);
}

export function formatDecimal(value, fractionDigits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }

  return new Intl.NumberFormat("gl-ES", {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits
  }).format(value);
}

export function formatPercentNumber(value, fractionDigits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }

  return `${formatDecimal(value, fractionDigits)}%`;
}

export function getYearFromDate(dateString) {
  return Number(String(dateString).slice(0, 4));
}

export function normalizeText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

export function sortRowsByDate(rows) {
  return [...(rows || [])].sort((a, b) => new Date(a.id_fecha) - new Date(b.id_fecha));
}

export function dedupeYears(rows) {
  return [...new Set((rows || []).map(row => getYearFromDate(row.id_fecha)))].sort((a, b) => a - b);
}

export function renderKpis(container, items) {
  container.innerHTML = items
    .map(item => `
      <article class="panel kpi">
        <p class="kpi-label">${item.label}</p>
        <h2 class="kpi-value">${item.value}</h2>
        <div class="kpi-trend">${item.detail}</div>
      </article>
    `)
    .join("");
}

export function renderCharts(container, charts) {
  container.innerHTML = charts
    .map(chart => `
      <article class="panel chart-card ${chart.wide ? "wide" : ""}">
        <h2>${chart.title}</h2>
        <p>${chart.description}</p>
        <div class="chart-wrap ${chart.size || ""}">
          <canvas id="${chart.id}"></canvas>
        </div>
      </article>
    `)
    .join("");
}

export function createChart(canvasId, config) {
  const canvas = document.getElementById(canvasId);
  const existing = Chart.getChart(canvas);
  if (existing) {
    existing.destroy();
  }
  return new Chart(canvas, config);
}

export function destroyCharts(charts) {
  Object.values(charts).forEach(chart => {
    if (chart) {
      chart.destroy();
    }
  });
}

export function sumBy(rows, labelKey, valueKey) {
  const totals = new Map();

  (rows || []).forEach(row => {
    const label = row[labelKey] || "Sen clasificar";
    totals.set(label, (totals.get(label) || 0) + Number(row[valueKey] || 0));
  });

  return [...totals.entries()]
    .map(([label, value]) => ({ label, value }))
    .sort((a, b) => b.value - a.value || a.label.localeCompare(b.label, "gl"));
}
