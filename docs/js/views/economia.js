import {
  chartDefaults,
  createChart,
  formatCurrency,
  formatNumber,
  formatPercent,
  getYearFromDate,
  normalizeText,
  renderCharts,
  renderKpis,
  sortRowsByDate,
  sumBy
} from "../utils.js";

const PALETTE = ["#123f73", "#1f5f9d", "#3e87c9", "#33a17b", "#1f7a63", "#c68b2e", "#ba4a6b", "#8eb9a8"];

function isTotalLabel(value) {
  const normalized = normalizeText(value);
  return normalized === "total" || normalized.startsWith("total ") || normalized.includes(" total");
}

function withoutTotals(rows, labelKey) {
  const filtered = (rows || []).filter(row => !isTotalLabel(row[labelKey]));
  return filtered.length ? filtered : rows || [];
}

function preferTotalRows(rows, labelKey) {
  const totalRows = (rows || []).filter(row => isTotalLabel(row[labelKey]));
  return totalRows.length ? totalRows : rows || [];
}

function getEconomy(municipioSeries) {
  return municipioSeries.economy || {};
}

function getRowsForYear(rowsByYear, year) {
  return rowsByYear?.[String(year)] || [];
}

function getAvailableYears(municipioSeries) {
  const economy = getEconomy(municipioSeries);
  const sectorYears = Object.keys(economy.companies_by_sector_year || {}).map(Number);
  const employeeYears = Object.keys(economy.companies_by_employee_year || {}).map(Number);
  const macroYears = (economy.macros || []).map(row => getYearFromDate(row.id_fecha));

  return [...new Set([...sectorYears, ...employeeYears, ...macroYears])].sort((a, b) => a - b);
}

function getMacroForYear(macros, selectedYear) {
  return [...(macros || [])]
    .filter(row => getYearFromDate(row.id_fecha) <= Number(selectedYear))
    .sort((a, b) => new Date(b.id_fecha) - new Date(a.id_fecha))[0] || null;
}

function buildSectorTotals(rows) {
  const baseRows = withoutTotals(preferTotalRows(rows, "tipo_empresa"), "sector");
  return sumBy(baseRows, "sector", "empresas_total");
}

function buildTypeTotals(rows) {
  const baseRows = withoutTotals(preferTotalRows(rows, "sector"), "tipo_empresa");
  return sumBy(baseRows, "tipo_empresa", "empresas_total");
}

function buildEmployeeTotals(rows) {
  return sumBy(withoutTotals(rows, "rango"), "rango", "empresas_total");
}

function getCompaniesTotal(sectorRows, employeeRows) {
  const employeeTotal = (employeeRows || []).find(row => isTotalLabel(row.rango));
  if (employeeTotal) {
    return Number(employeeTotal.empresas_total || 0);
  }

  const sectorTotal = (sectorRows || []).find(row => isTotalLabel(row.sector) && isTotalLabel(row.tipo_empresa));
  if (sectorTotal) {
    return Number(sectorTotal.empresas_total || 0);
  }

  return buildSectorTotals(sectorRows).reduce((sum, row) => sum + Number(row.value || 0), 0);
}

function buildCompanyTrend(economy) {
  const years = getAvailableYears({ economy });

  return years
    .map(year => {
      const sectorRows = getRowsForYear(economy.companies_by_sector_year, year);
      const employeeRows = getRowsForYear(economy.companies_by_employee_year, year);
      return {
        year,
        value: getCompaniesTotal(sectorRows, employeeRows)
      };
    })
    .filter(row => row.value > 0);
}

function buildKpis(economy, selectedYear, sectorRows, employeeRows) {
  const macros = sortRowsByDate(economy.macros || []);
  const macroRow = getMacroForYear(macros, selectedYear);
  const trend = buildCompanyTrend(economy);
  const current = trend.find(row => row.year === Number(selectedYear)) || [...trend].reverse().find(row => row.year <= Number(selectedYear));
  const previous = [...trend].reverse().find(row => current && row.year < current.year);
  const variation = current && previous && previous.value ? (current.value - previous.value) / previous.value : null;
  const companiesTotal = current?.value || getCompaniesTotal(sectorRows, employeeRows);
  const macroYear = macroRow ? getYearFromDate(macroRow.id_fecha) : null;

  return [
    {
      label: "Empresas totais",
      value: companiesTotal ? formatNumber(companiesTotal) : "--",
      detail: current ? `Rexistro de ${current.year}` : "Sen rexistro empresarial para o ano"
    },
    {
      label: "Variacion anual",
      value: variation === null ? "--" : formatPercent(variation),
      detail: previous && current ? `${formatNumber(current.value - previous.value)} empresas vs ${previous.year}` : "Sen ano previo comparable"
    },
    {
      label: "PIB per capita",
      value: macroRow?.pib_per_capita ? formatCurrency(macroRow.pib_per_capita) : "--",
      detail: macroYear ? `Ultimo dato dispoñible: ${macroYear}` : "Sen dato macroeconomico"
    },
    {
      label: "Renda per capita",
      value: macroRow?.renta_bruta_per_capita ? formatCurrency(macroRow.renta_bruta_per_capita) : "--",
      detail: macroYear ? `Ultimo dato dispoñible: ${macroYear}` : "Sen dato macroeconomico"
    }
  ];
}

function renderHorizontalBar(canvasId, rows, label) {
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

function renderDoughnut(canvasId, rows) {
  const total = rows.reduce((sum, row) => sum + Number(row.value || 0), 0);

  return createChart(canvasId, {
    type: "doughnut",
    data: {
      labels: rows.map(row => row.label),
      datasets: [
        {
          data: rows.map(row => row.value),
          backgroundColor: rows.map((_, index) => PALETTE[index % PALETTE.length]),
          borderColor: "#ffffff",
          borderWidth: 3
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      layout: { padding: 8 },
      plugins: {
        legend: {
          position: "bottom",
          labels: { usePointStyle: true, boxWidth: 10, color: "#4b6884", padding: 14 }
        },
        tooltip: {
          ...chartDefaults.plugins.tooltip,
          callbacks: {
            label(context) {
              const value = Number(context.parsed || 0);
              const share = total ? value / total : 0;
              return `${context.label}: ${formatNumber(value)} empresas · ${formatPercent(share)}`;
            }
          }
        }
      }
    }
  });
}

function renderMacroEvolution(macros) {
  const rows = sortRowsByDate(macros || []);

  return createChart("macro-trend-chart", {
    type: "line",
    data: {
      labels: rows.map(row => getYearFromDate(row.id_fecha)),
      datasets: [
        {
          label: "PIB per capita",
          data: rows.map(row => row.pib_per_capita),
          borderColor: "#123f73",
          backgroundColor: "rgba(31, 95, 157, 0.12)",
          tension: 0.28,
          fill: true,
          pointRadius: 3
        },
        {
          label: "Renda per capita",
          data: rows.map(row => row.renta_bruta_per_capita),
          borderColor: "#33a17b",
          backgroundColor: "#33a17b",
          tension: 0.28,
          pointRadius: 3
        }
      ]
    },
    options: {
      ...chartDefaults,
      plugins: {
        ...chartDefaults.plugins,
        tooltip: {
          ...chartDefaults.plugins.tooltip,
          callbacks: {
            label(context) {
              return `${context.dataset.label}: ${formatCurrency(context.parsed.y)}`;
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
              return formatCurrency(value);
            }
          }
        }
      }
    }
  });
}

export const economyView = {
  label: "Economia",
  eyebrow: "Comarca de Verin · Atlas economico",
  description: "Indicadores para analizar tecido empresarial, estrutura por sectores e asalariados, e evolucion de PIB e renda per capita.",
  filtersDescription: "Explora cada municipio e cambia o ano de detalle para comparar empresas e indicadores macroeconomicos.",
  note: "As graficas por sector, asalariados e tipo de empresa usan o ano seleccionado. PIB e renda mostran a serie macroeconomica dispoñible.",
  getYears: getAvailableYears,
  render({ municipioSeries, selectedYear, kpiGrid, chartsGrid }) {
    const economy = getEconomy(municipioSeries);
    const sectorRows = getRowsForYear(economy.companies_by_sector_year, selectedYear);
    const employeeRows = getRowsForYear(economy.companies_by_employee_year, selectedYear);
    const sectorTotals = buildSectorTotals(sectorRows);
    const employeeTotals = buildEmployeeTotals(employeeRows);
    const typeTotals = buildTypeTotals(sectorRows);

    if (!sectorRows.length && !employeeRows.length && !(economy.macros || []).length) {
      throw new Error("Ese municipio non ten rexistros no datamart economico.");
    }

    renderKpis(kpiGrid, buildKpis(economy, selectedYear, sectorRows, employeeRows));
    renderCharts(chartsGrid, [
      { id: "sector-chart", title: "Empresas por sector", description: "Distribucion do numero de empresas por sector economico.", size: "tall" },
      { id: "employee-chart", title: "Empresas por asalariados", description: "Tecido empresarial segundo o rango de persoas asalariadas.", size: "tall" },
      { id: "type-chart", title: "Tipo de empresas", description: "Reparto por personalidade xuridica ou tipo empresarial." },
      { id: "macro-trend-chart", title: "Evolucion de PIB e renda", description: "Serie de PIB per capita e renda bruta per capita.", wide: true, size: "medium" }
    ]);

    return {
      sector: renderHorizontalBar("sector-chart", sectorTotals, "Empresas"),
      employees: renderHorizontalBar("employee-chart", employeeTotals, "Empresas"),
      type: renderDoughnut("type-chart", typeTotals),
      macro: renderMacroEvolution(economy.macros || [])
    };
  }
};
