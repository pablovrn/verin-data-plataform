import {
  chartDefaults,
  createChart,
  dedupeYears,
  formatDecimal,
  formatNumber,
  formatPercent,
  formatPercentNumber,
  getYearFromDate,
  normalizeText,
  renderCharts,
  renderKpis,
  sortRowsByDate
} from "../utils.js";

const AGE_BUCKETS = [
  { key: "0-14", label: "0-14", min: 0, max: 14, color: "#9bc6e8" },
  { key: "15-64", label: "15-64", min: 15, max: 64, color: "#1f5f9d" },
  { key: "65+", label: "65+", min: 65, max: Infinity, color: "#33a17b" }
];

function getAgeRangeBounds(label) {
  const normalized = normalizeText(label);

  if (normalized.includes("e mais")) {
    const minValue = Number.parseInt(normalized, 10);
    return Number.isNaN(minValue) ? null : { min: minValue, max: Infinity };
  }

  const match = normalized.match(/(\d+)\s*-\s*(\d+)/);
  return match ? { min: Number(match[1]), max: Number(match[2]) } : null;
}

function getAgeBucketForRow(row) {
  const bounds = getAgeRangeBounds(row.rango);
  if (!bounds) {
    return null;
  }

  return AGE_BUCKETS.find(bucket => bounds.min >= bucket.min && bounds.max <= bucket.max) || null;
}

function buildAgeTrend(ageByYear) {
  return Object.entries(ageByYear || {})
    .sort((a, b) => Number(a[0]) - Number(b[0]))
    .map(([year, rows]) => {
      const totals = AGE_BUCKETS.reduce((accumulator, bucket) => {
        accumulator[bucket.key] = 0;
        return accumulator;
      }, {});

      (rows || []).forEach(row => {
        const bucket = getAgeBucketForRow(row);
        if (bucket) {
          totals[bucket.key] += Number(row.poblacion_total || 0);
        }
      });

      return { year: Number(year), ...totals };
    });
}

function buildAgeTrendPercentages(rows) {
  return (rows || []).map(row => {
    const total = AGE_BUCKETS.reduce((sum, bucket) => sum + Number(row[bucket.key] || 0), 0);
    const percentages = AGE_BUCKETS.reduce((accumulator, bucket) => {
      accumulator[bucket.key] = total ? (Number(row[bucket.key] || 0) / total) * 100 : 0;
      return accumulator;
    }, {});

    return { year: row.year, ...percentages };
  });
}

function getAgingValues(rows) {
  return (rows || []).reduce(
    (accumulator, row) => {
      const bucket = getAgeBucketForRow(row);
      const population = Number(row.poblacion_total || 0);

      if (bucket?.key === "0-14") {
        accumulator.youngerThan15 += population;
      }

      if (bucket?.key === "65+") {
        accumulator.olderThan64 += population;
      }

      return accumulator;
    },
    { youngerThan15: 0, olderThan64: 0 }
  );
}

function getAgingIndex(rows) {
  const values = getAgingValues(rows);
  return values.youngerThan15 ? (values.olderThan64 / values.youngerThan15) * 100 : null;
}

function getAgingDetail(rows) {
  const values = getAgingValues(rows);

  if (!values.youngerThan15) {
    return "Sen base menor de 15 anos para calcular o indice";
  }

  return `${formatNumber(values.olderThan64)} persoas de 65+ por ${formatNumber(values.youngerThan15)} menores de 15 anos`;
}

function classifyOrigin(label) {
  const normalized = normalizeText(label);

  if (
    normalized.includes("no concello de residencia")
    || normalized.includes("mesma provincia")
    || normalized.includes("provincia de galicia")
  ) {
    return "galicia";
  }

  return normalized.includes("estranxeiro") ? "foreign" : "spain";
}

function getOriginColor(row, index) {
  const category = classifyOrigin(row.nombre);

  if (category === "galicia") {
    return ["#123f73", "#1f5f9d", "#3e87c9"][index % 3];
  }

  return category === "foreign" ? "#33a17b" : "#8eb9a8";
}

function getAvailableYears(municipioSeries) {
  const populationYears = dedupeYears(municipioSeries.population || []);
  const ageYears = Object.keys(municipioSeries.age_by_year || {}).map(Number);
  const originYears = Object.keys(municipioSeries.origin_by_year || {}).map(Number);

  return [...new Set([...populationYears, ...ageYears, ...originYears])].sort((a, b) => a - b);
}

function mergeVitalSeries(years, births, deaths) {
  return years.map(year => ({
    year,
    births: births.find(item => getYearFromDate(item.id_fecha) === year)?.nacimientos_total || 0,
    deaths: deaths.find(item => getYearFromDate(item.id_fecha) === year)?.defunciones_total || 0
  }));
}

function buildKpis(populationRows, birthsRows, deathsRows, ageRows, selectedYear) {
  const currentRow = [...populationRows].reverse().find(row => getYearFromDate(row.id_fecha) === Number(selectedYear));
  const previousRow = [...populationRows].reverse().find(row => getYearFromDate(row.id_fecha) < Number(selectedYear));
  const birthsRow = birthsRows.find(row => getYearFromDate(row.id_fecha) === Number(selectedYear));
  const deathsRow = deathsRows.find(row => getYearFromDate(row.id_fecha) === Number(selectedYear));
  const agingIndex = getAgingIndex(ageRows);
  const total = currentRow?.poblacion_total || 0;
  const hombres = currentRow?.hombres || 0;
  const mujeres = currentRow?.mujeres || 0;
  const variation = currentRow && previousRow ? total - (previousRow.poblacion_total || 0) : null;
  const births = birthsRow?.nacimientos_total;
  const deaths = deathsRow?.defunciones_total;
  const balance = Number(births || 0) - Number(deaths || 0);

  return [
    {
      label: "Poboacion total",
      value: currentRow ? formatNumber(total) : "--",
      detail: !currentRow
        ? "Non hai rexistro de poboacion para o ano seleccionado"
        : variation === null
          ? "Primeiro ano dispoñible na serie"
          : `${variation >= 0 ? "+" : ""}${formatNumber(variation)} vs ${getYearFromDate(previousRow.id_fecha)}`
    },
    {
      label: "Homes",
      value: currentRow ? formatNumber(hombres) : "--",
      detail: currentRow ? `${formatPercent(total ? hombres / total : 0)} do total` : "--"
    },
    {
      label: "Mulleres",
      value: currentRow ? formatNumber(mujeres) : "--",
      detail: currentRow ? `${formatPercent(total ? mujeres / total : 0)} do total` : "--"
    },
    {
      label: "Indice de envellecemento",
      value: agingIndex === null ? "--" : formatDecimal(agingIndex),
      detail: getAgingDetail(ageRows)
    },
    {
      label: "Saldo vexetativo",
      value: birthsRow || deathsRow ? `${balance >= 0 ? "+" : ""}${formatNumber(balance)}` : "--",
      detail: birthsRow || deathsRow
        ? `${formatNumber(births || 0)} nacementos e ${formatNumber(deaths || 0)} defuncions`
        : "Sen rexistro de nacementos e defuncions para ese ano"
    }
  ];
}

function renderPopulationChart(rows) {
  return createChart("population-trend-chart", {
    type: "line",
    data: {
      labels: rows.map(row => getYearFromDate(row.id_fecha)),
      datasets: [
        {
          label: "Poboacion total",
          data: rows.map(row => row.poblacion_total),
          borderColor: "#123f73",
          backgroundColor: "rgba(31, 95, 157, 0.12)",
          tension: 0.3,
          fill: true,
          pointRadius: 3
        },
        {
          label: "Homes",
          data: rows.map(row => row.hombres),
          borderColor: "#3e87c9",
          backgroundColor: "#3e87c9",
          tension: 0.3,
          pointRadius: 2
        },
        {
          label: "Mulleres",
          data: rows.map(row => row.mujeres),
          borderColor: "#33a17b",
          backgroundColor: "#33a17b",
          tension: 0.3,
          pointRadius: 2
        }
      ]
    },
    options: chartDefaults
  });
}

function renderVitalChart(rows) {
  return createChart("vital-chart", {
    type: "bar",
    data: {
      labels: rows.map(row => row.year),
      datasets: [
        { label: "Nacementos", data: rows.map(row => row.births), backgroundColor: "rgba(62, 135, 201, 0.85)", borderRadius: 8 },
        { label: "Defuncions", data: rows.map(row => row.deaths), backgroundColor: "rgba(31, 122, 99, 0.8)", borderRadius: 8 }
      ]
    },
    options: chartDefaults
  });
}

function renderAgeTrendChart(rows) {
  const percentageRows = buildAgeTrendPercentages(rows);

  return createChart("age-trend-chart", {
    type: "line",
    data: {
      labels: percentageRows.map(row => row.year),
      datasets: AGE_BUCKETS.map(bucket => ({
        label: bucket.label,
        data: percentageRows.map(row => row[bucket.key] || 0),
        borderColor: bucket.color,
        backgroundColor: bucket.color,
        tension: 0.28,
        fill: false,
        pointRadius: 2.5,
        pointHoverRadius: 4
      }))
    },
    options: {
      ...chartDefaults,
      plugins: {
        ...chartDefaults.plugins,
        tooltip: {
          ...chartDefaults.plugins.tooltip,
          callbacks: {
            label(context) {
              return `${context.dataset.label}: ${formatPercentNumber(context.parsed.y)}`;
            }
          }
        }
      },
      scales: {
        ...chartDefaults.scales,
        y: {
          ...chartDefaults.scales.y,
          min: 0,
          max: 100,
          ticks: {
            color: "#5f7891",
            padding: 10,
            callback(value) {
              return `${value}%`;
            }
          }
        }
      }
    }
  });
}

function renderAgeChart(rows) {
  return createChart("age-chart", {
    type: "bar",
    data: {
      labels: rows.map(row => row.rango || `Grupo ${row.id_grupo_edad}`),
      datasets: [
        {
          label: "Homes",
          data: rows.map(row => row.hombres || 0),
          backgroundColor: "rgba(62, 135, 201, 0.82)",
          borderRadius: 6,
          borderSkipped: false,
          categoryPercentage: 0.72,
          barPercentage: 0.92,
          clip: false
        },
        {
          label: "Mulleres",
          data: rows.map(row => row.mujeres || 0),
          backgroundColor: "rgba(51, 161, 123, 0.82)",
          borderRadius: 6,
          borderSkipped: false,
          categoryPercentage: 0.72,
          barPercentage: 0.92,
          clip: false
        }
      ]
    },
    options: {
      ...chartDefaults,
      indexAxis: "y",
      layout: { padding: { top: 18, right: 18, bottom: 18, left: 8 } },
      scales: {
        x: { ...chartDefaults.scales.y, grace: "10%" },
        y: {
          ...chartDefaults.scales.x,
          offset: true,
          ticks: { color: "#5f7891", padding: 10, autoSkip: false }
        }
      }
    }
  });
}

function renderOriginChart(rows) {
  const total = rows.reduce((sum, row) => sum + Number(row.poblacion_total || 0), 0);

  return createChart("origin-chart", {
    type: "doughnut",
    data: {
      labels: rows.map(row => row.nombre || `Lugar ${row.id_lugar_nacimiento}`),
      datasets: [
        {
          data: rows.map(row => row.poblacion_total || 0),
          backgroundColor: rows.map((row, index) => getOriginColor(row, index)),
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
          backgroundColor: "rgba(14, 44, 74, 0.96)",
          titleColor: "#ffffff",
          bodyColor: "#eff7ff",
          padding: 12,
          cornerRadius: 10,
          callbacks: {
            label(context) {
              const value = Number(context.parsed || 0);
              const share = total ? (value / total) * 100 : 0;
              return `${context.label}: ${formatNumber(value)} persoas · ${formatPercentNumber(share)}`;
            }
          }
        }
      }
    }
  });
}

export const populationView = {
  label: "Poboacion",
  eyebrow: "Comarca de Verin · Atlas demografico",
  description: "Un panel visual para seguir a evolucion demografica da comarca con foco en poboacion, saldo vexetativo, estrutura por idades, indice de envellecemento e lugar de nacemento.",
  filtersDescription: "Explora cada concello e cambia o ano de detalle para comparar estrutura e procedencia.",
  note: "A evolucion temporal usa toda a serie do municipio. Os graficos de idade e procedencia usan o ano seleccionado. So existen datos de idade e procedencia para 2001, 2011 e dende 2021 en diante.",
  getYears: getAvailableYears,
  render({ municipioSeries, selectedYear, kpiGrid, chartsGrid }) {
    const populationRows = sortRowsByDate(municipioSeries.population || []);
    const birthsRows = sortRowsByDate(municipioSeries.births || []);
    const deathsRows = sortRowsByDate(municipioSeries.deaths || []);
    const ageTrendRows = buildAgeTrend(municipioSeries.age_by_year || {});
    const years = getAvailableYears(municipioSeries);
    const ageRows = municipioSeries.age_by_year?.[String(selectedYear)] || [];
    const originRows = municipioSeries.origin_by_year?.[String(selectedYear)] || [];

    if (!populationRows.length) {
      throw new Error("Ese municipio non ten rexistros en fact_poblacion.");
    }

    renderKpis(kpiGrid, buildKpis(populationRows, birthsRows, deathsRows, ageRows, selectedYear));
    renderCharts(chartsGrid, [
      { id: "population-trend-chart", title: "Evolucion da poboacion", description: "Serie anual de poboacion total, homes e mulleres." },
      { id: "vital-chart", title: "Nacementos vs defuncions", description: "Comparativa anual para detectar o saldo vexetativo do municipio." },
      { id: "age-trend-chart", title: "Evolucion por grupos de idade", description: "Seguimento dos grandes tramos demograficos: infancia, idade activa e maiores.", wide: true, size: "medium" },
      { id: "age-chart", title: "Estrutura por idade", description: "Distribucion da poboacion no ano seleccionado.", size: "tall" },
      { id: "origin-chart", title: "Lugar de nacemento", description: "Os nados en Galicia comparten tons proximos para diferencialos do resto de Espana e do estranxeiro.", size: "tall" }
    ]);

    return {
      population: renderPopulationChart(populationRows),
      vital: renderVitalChart(mergeVitalSeries(years, birthsRows, deathsRows)),
      ageTrend: renderAgeTrendChart(ageTrendRows),
      age: renderAgeChart(ageRows),
      origin: renderOriginChart(originRows)
    };
  }
};
