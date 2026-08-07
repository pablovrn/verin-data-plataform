import {
  chartDefaults,
  createChart,
  formatDecimal,
  formatNumber,
  renderCharts,
  renderKpis,
  getYearFromDate,
  sumBy
} from "../utils.js";

const PALETTE = ["#123f73", "#1f5f9d", "#3e87c9", "#33a17b", "#1f7a63", "#c68b2e", "#ba4a6b", "#8eb9a8"];

function getServices(municipioSeries) {
  return municipioSeries.services || {};
}

function getRowsForYear(rowsByYear, year) {
  return rowsByYear?.[String(year)] || [];
}

function getAvailableYears(municipioSeries) {
  const services = getServices(municipioSeries);
  const healthYears = Object.keys(services.healthcare_by_year || {}).map(Number);
  const educationYears = Object.keys(services.education_by_year || {}).map(Number);

  return [...new Set([...healthYears, ...educationYears])].sort((a, b) => a - b);
}

function buildCategoryTotals(rows, labelKey) {
  return sumBy(
    (rows || []).map(row => ({
      ...row,
      label: row[labelKey] || row.label || "Sen clasificar"
    })),
    "label",
    "total"
  );
}

function buildTrendSeries(rowsByYear, labelKey) {
  const years = Object.keys(rowsByYear || {}).sort((a, b) => Number(a) - Number(b));
  const categories = [...new Set(years.flatMap(year => (rowsByYear[year] || []).map(row => row[labelKey] || row.label || "Sen clasificar")))];

  return {
    years,
    datasets: categories.map((category, index) => ({
      label: category,
      data: years.map(year => {
        const row = (rowsByYear[year] || []).find(item => (item[labelKey] || item.label || "Sen clasificar") === category);
        return Number(row?.total || 0);
      }),
      borderColor: PALETTE[index % PALETTE.length],
      backgroundColor: PALETTE[index % PALETTE.length],
      tension: 0.28,
      fill: false,
      pointRadius: 3,
      pointHoverRadius: 5
    }))
  };
}

function renderHorizontalBar(canvasId, rows, label, title) {
  return createChart(canvasId, {
    type: "bar",
    data: {
      labels: rows.map(row => row.label),
      datasets: [
        {
          label,
          data: rows.map(row => row.value),
          backgroundColor: rows.map((_, index) => PALETTE[index % PALETTE.length]),
          borderRadius: 7,
          borderSkipped: false,
          categoryPercentage: 0.72,
          barPercentage: 0.92
        }
      ]
    },
    options: {
      ...chartDefaults,
      indexAxis: "y",
      plugins: {
        ...chartDefaults.plugins,
        legend: { display: false }
      },
      scales: {
        x: { ...chartDefaults.scales.y, grace: "10%" },
        y: {
          ...chartDefaults.scales.x,
          offset: true,
          ticks: {
            color: "#5f7891",
            padding: 10,
            autoSkip: false
          }
        }
      }
    }
  });
}

function renderTrendChart(canvasId, trendData) {
  return createChart(canvasId, {
    type: "line",
    data: {
      labels: trendData.years.map(year => Number(year)),
      datasets: trendData.datasets
    },
    options: {
      ...chartDefaults,
      plugins: {
        ...chartDefaults.plugins,
        tooltip: {
          ...chartDefaults.plugins.tooltip,
          callbacks: {
            label(context) {
              return `${context.dataset.label}: ${formatNumber(context.parsed.y)}`;
            }
          }
        }
      },
      scales: {
        ...chartDefaults.scales,
        y: {
          ...chartDefaults.scales.y,
          ticks: {
            color: "#5f7891",
            padding: 10,
            callback(value) {
              return formatNumber(value);
            }
          }
        }
      }
    }
  });
}

function buildKpis(services, selectedYear, populationRows) {
  const healthRows = getRowsForYear(services.healthcare_by_year, selectedYear);
  const educationRows = getRowsForYear(services.education_by_year, selectedYear);
  const healthTotal = (healthRows || []).reduce((sum, row) => sum + Number(row.total || 0), 0);
  const educationTotal = (educationRows || []).reduce((sum, row) => sum + Number(row.total || 0), 0);
  const populationRow = [...(populationRows || [])].reverse().find(row => getYearFromDate(row.id_fecha) === Number(selectedYear));
  const populationTotal = Number(populationRow?.poblacion_total || 0);
  const ratio = populationTotal ? (healthTotal / populationTotal) * 100 : null;

  return [
    {
      label: "Personal sanitario total",
      value: healthTotal ? formatNumber(healthTotal) : "--",
      detail: healthRows.length ? `Datos de ${selectedYear}` : "Sen rexistro de personal sanitario para o ano"
    },
    {
      label: "Por cada 100 habitantes",
      value: ratio === null ? "--" : formatDecimal(ratio, 2),
      detail: populationTotal ? `Base: ${formatNumber(populationTotal)} habitantes` : "Sen poboacion para o ano seleccionado"
    },
    {
      label: "Alumnos totais",
      value: educationTotal ? formatNumber(educationTotal) : "--",
      detail: educationRows.length ? `Datos de ${selectedYear}` : "Sen rexistro de alumnos para o ano"
    }
  ];
}

export const servicesView = {
  label: "Servicios",
  eyebrow: "Comarca de Verin · Servizos e educacion",
  description: "Indicadores para seguir a dotacion de personal sanitario e o volume de alumnos por niveis educativos.",
  filtersDescription: "Explora cada concello e cambia o ano de detalle para comparar recursos sanitarios e educativos.",
  note: "As distribucions usan o ano seleccionado. A evolución temporal mostra toda a serie dispoñible para cada categoría. Sen datos significa que non hai rexistros para ese ano.",
  getYears: getAvailableYears,
  render({ municipioSeries, selectedYear, kpiGrid, chartsGrid }) {
    const services = getServices(municipioSeries);
    const healthRows = getRowsForYear(services.healthcare_by_year, selectedYear);
    const educationRows = getRowsForYear(services.education_by_year, selectedYear);
    const healthDistribution = buildCategoryTotals(healthRows, "tipo_sanitario");
    const educationDistribution = buildCategoryTotals(educationRows, "tipo_educacion");
    const healthTrend = buildTrendSeries(services.healthcare_by_year || {}, "tipo_sanitario");
    const educationTrend = buildTrendSeries(services.education_by_year || {}, "tipo_educacion");

    if (!healthRows.length && !educationRows.length) {
      throw new Error("Ese municipio non ten rexistros no datamart de servizos.");
    }

    renderKpis(kpiGrid, buildKpis(services, selectedYear, municipioSeries.population || []));
    renderCharts(chartsGrid, [
      { id: "health-distribution-chart", title: "Distribucion de persoal sanitario", description: "Reparto do personal sanitario por tipo para o ano seleccionado.", size: "tall" },
      { id: "health-trend-chart", title: "Evolucion do persoal sanitario", description: "Serie temporal do persoal sanitario por tipo.", size: "tall" },
      { id: "education-distribution-chart", title: "Distribucion de alumnos por nivel", description: "Reparto de alumnos por nivel educativo para o ano seleccionado.", size: "tall" },
      { id: "education-trend-chart", title: "Evolucion de alumnos por nivel", description: "Serie temporal dos alumnos por nivel educativo.", size: "tall" }
    ]);

    return {
      healthDistribution: renderHorizontalBar("health-distribution-chart", healthDistribution, "Persoas", "Distribucion"),
      healthTrend: renderTrendChart("health-trend-chart", healthTrend),
      educationDistribution: renderHorizontalBar("education-distribution-chart", educationDistribution, "Alumnos", "Distribucion"),
      educationTrend: renderTrendChart("education-trend-chart", educationTrend)
    };
  }
};
