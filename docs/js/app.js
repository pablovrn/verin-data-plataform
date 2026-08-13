import { economyView } from "./views/economia.js";
import { populationView } from "./views/poblacion.js";
import { servicesView } from "./views/servicios.js";
import { normalizeText, destroyCharts } from "./utils.js";

const DATA_URL = "./data/dashboard.json";

const views = {
  population: populationView,
  economy: economyView,
  services: servicesView
};

const state = {
  dataset: null,
  municipios: [],
  selectedMunicipio: "",
  selectedYear: "",
  selectedScope: "population",
  charts: {}
};

const CHAT_URL = "http://localhost:8000/chat";

const elements = {
  status: document.getElementById("status"),
  heroEyebrow: document.getElementById("hero-eyebrow"),
  heroDescription: document.getElementById("hero-description"),
  filtersDescription: document.getElementById("filters-description"),
  scopeNote: document.getElementById("scope-note"),
  municipioSelect: document.getElementById("municipio-select"),
  yearSelect: document.getElementById("year-select"),
  kpiGrid: document.getElementById("kpi-grid"),
  chartsGrid: document.getElementById("charts-grid"),
  chatInput: document.getElementById("chat-input"),
  chatResponse: document.getElementById("chat-response"),
  chatSubmit: document.getElementById("chat-submit"),
  chatPanel: document.getElementById("chat-panel"),
  dashboardShell: document.querySelector(".dashboard-shell"),
  scopeTabs: [...document.querySelectorAll("[data-scope]")]
};

function setStatus(message, type = "") {
  elements.status.textContent = message;
  elements.status.className = type ? `status ${type}` : "status";
}

async function fetchJson(url) {
  const response = await fetch(url);
  let payload = null;

  try {
    payload = await response.json();
  } catch (error) {
    throw new Error("Non se puido ler o arquivo de datos do dashboard.");
  }

  if (!response.ok) {
    throw new Error(payload.error || "Non se puido completar a solicitude.");
  }

  return payload;
}

function fillMunicipioSelect(municipios) {
  elements.municipioSelect.innerHTML = municipios
    .map(municipio => `<option value="${municipio.id_municipio}">${municipio.nombre}</option>`)
    .join("");
  elements.municipioSelect.disabled = false;
}

function fillYearSelect(years) {
  elements.yearSelect.innerHTML = years
    .map(year => `<option value="${year}">${year}</option>`)
    .join("");
  elements.yearSelect.disabled = years.length === 0;
}

function getMunicipioSeries(municipioId) {
  return state.dataset?.series?.[String(municipioId)] || null;
}

function getDefaultMunicipio(payload, municipios) {
  return String(
    payload.default_municipio_id
      || municipios.find(item => normalizeText(item.nombre) === "verin")?.id_municipio
      || municipios[0].id_municipio
  );
}

function setActiveScope(scope) {
  state.selectedScope = scope;
  elements.scopeTabs.forEach(tab => {
    tab.classList.toggle("active", tab.dataset.scope === scope);
  });
}

async function renderDashboard(preferredYear = state.selectedYear) {
  const view = views[state.selectedScope];
  const municipioId = elements.municipioSelect.value;
  const municipioSeries = getMunicipioSeries(municipioId);
  const municipioNombre = state.municipios.find(item => String(item.id_municipio) === String(municipioId))?.nombre || "Municipio";

  if (!view || !municipioSeries) {
    return;
  }

  setStatus("Cargando series e actualizando graficos...");
  destroyCharts(state.charts);
  state.charts = {};

  try {
    const years = view.getYears(municipioSeries);
    if (!years.length) {
      throw new Error(`Non hai datos dispoñibles para ${view.label.toLowerCase()} neste municipio.`);
    }

    const selectedYear = preferredYear && years.includes(Number(preferredYear))
      ? Number(preferredYear)
      : years[years.length - 1];

    state.selectedMunicipio = String(municipioId);
    state.selectedYear = String(selectedYear);

    elements.heroEyebrow.textContent = view.eyebrow;
    elements.heroDescription.textContent = view.description;
    elements.filtersDescription.textContent = view.filtersDescription;
    elements.scopeNote.textContent = view.note;
    fillYearSelect(years);
    elements.yearSelect.value = String(selectedYear);

    state.charts = view.render({
      municipioSeries,
      selectedYear,
      kpiGrid: elements.kpiGrid,
      chartsGrid: elements.chartsGrid
    });

    const generatedAt = state.dataset?.generated_at ? ` Datos exportados: ${state.dataset.generated_at}.` : "";
    setStatus(`Datos de ${view.label.toLowerCase()} cargados para ${municipioNombre} (${selectedYear}).${generatedAt}`);
  } catch (error) {
    elements.kpiGrid.innerHTML = "";
    elements.chartsGrid.innerHTML = `<div class="panel empty-state">${error.message}</div>`;
    setStatus(error.message, "error");
  }
}

async function connectAndBootstrap() {
  setStatus("Cargando datos estaticos do dashboard...");

  try {
    const payload = await fetchJson(DATA_URL);
    const municipios = payload.municipios || [];

    if (!municipios.length) {
      throw new Error("Non se atoparon municipios no arquivo de datos.");
    }

    state.dataset = payload;
    state.municipios = municipios;
    fillMunicipioSelect(municipios);

    const defaultMunicipio = getDefaultMunicipio(payload, municipios);
    elements.municipioSelect.value = defaultMunicipio;
    await renderDashboard();
  } catch (error) {
    setStatus(error.message, "error");
  }
}

elements.municipioSelect.addEventListener("change", async () => {
  await renderDashboard(elements.yearSelect.value);
});

elements.yearSelect.addEventListener("change", async event => {
  await renderDashboard(event.target.value);
});

elements.scopeTabs.forEach(tab => {
  tab.addEventListener("click", async () => {
    setActiveScope(tab.dataset.scope);
    if (tab.dataset.scope === "chat") {
      elements.dashboardShell.classList.add("hidden");
      elements.chatPanel.classList.remove("hidden");
      setStatus("Chat IA activo. Escribe a túa pregunta e pulsa Enviar.");
      return;
    }

    elements.dashboardShell.classList.remove("hidden");
    elements.chatPanel.classList.add("hidden");
    await renderDashboard();
  });
});

async function sendChatMessage() {
  const message = elements.chatInput.value.trim();
  if (!message) {
    elements.chatResponse.textContent = "Escribe unha pregunta antes de enviar.";
    return;
  }

  elements.chatSubmit.disabled = true;
  elements.chatResponse.textContent = "Enviando a túa pregunta...";

  try {
    const response = await fetch(CHAT_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ message })
    });

    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "O servidor devolveu un erro inesperado.");
    }

    elements.chatResponse.textContent = payload.answer || "O servidor non devolveu ningunha resposta.";
  } catch (error) {
    elements.chatResponse.textContent = `Erro no chat: ${error.message}`;
  } finally {
    elements.chatSubmit.disabled = false;
  }
}

elements.chatSubmit.addEventListener("click", sendChatMessage);
elements.chatInput.addEventListener("keydown", event => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    sendChatMessage();
  }
});

connectAndBootstrap();
