# Meta Reversa para Planejamento de Aberturas em Operações de Importação

Pipeline de dados em Python que converte metas mensais de faturamento em um plano operacional: quantos processos de importação devem ser abertos a cada mês, por modal de transporte e tipo de serviço, para atingir os objetivos de receita.

**Trabalho de Conclusão de Curso** - MBA em Data Science e Analytics (USP/Esalq)
**Autor:** Pedro Henrique Scholles

---

## Visão Geral

O modelo de **meta reversa** parte das metas de receita e trabalha de trás para frente. Responde à pergunta: **"dada uma meta de faturamento futura, quantos processos precisamos abrir hoje?"** — invertendo a lógica tradicional de forecasting.

Cinco algoritmos encadeados:

1. **Análise Histórica** — calcula parâmetros estruturais (lead times, shares, tickets)
2. **Distribuição Temporal** — determina **quando** abrir os processos (via lead time mediano)
3. **Meta Reversa** — calcula **quantos** processos abrir (via ticket médio + ceiling)
4. **Backtesting** — valida o modelo em dois splits temporais (2023→2024 e 2023+2024→2025)
5. **Sensibilidade** — quantifica o impacto de variações nos parâmetros

## Arquitetura do Pipeline

```
data/db_base_processos.csv ──┐
                              ├──▶ [0] Data Wrangling
data/db_metas.csv ───────────┘          │
                                        ▼
                                [1] Análise Histórica
                                   (lead times, tickets, shares)
                                        │
                                        ▼
                                [2] Distribuição Temporal
                                   (meta → mês de abertura)
                                        │
                                        ▼
                                [3] Meta Reversa
                                   (meta → nº de processos)
                                        │
                                        ▼
                                [4] Backtesting
                                   (validação out-of-sample)
                                        │
                                        ▼
                                [5] Análise de Sensibilidade
                                   (variações de ticket e lead time)
                                        │
                                        ▼
                                  outputs/ (10 CSVs)
```

## Estrutura do Projeto

```
projeto_meta_reversa/
├── main.py                     # Orquestração do pipeline
├── requirements.txt            # Dependências
├── src/
│   ├── __init__.py
│   ├── data_wrangling.py       # ETL e limpeza de dados
│   ├── analise_historica.py    # Cálculo de métricas históricas
│   ├── distribuicao_temporal.py# Planejamento temporal de aberturas
│   ├── meta_reversa.py         # Conversão de meta em processos
│   ├── backtesting.py          # Validação out-of-sample (2 splits)
│   └── analise_sensibilidade.py# Sensibilidade a variações
├── data/
│   ├── db_base_processos.csv   # Histórico de processos
│   └── db_metas.csv            # Metas mensais 2026
└── outputs/                    # Resultados gerados
```

## Como Executar

**Pré-requisitos:** Python 3.10+ com `pandas >= 2.2, < 3.0`.

```bash
# Instalar dependências
pip install -r requirements.txt

# Executar o pipeline
python main.py
```

O pipeline imprime logs detalhados no console e salva os resultados em `outputs/`.

---

## Etapas do Pipeline

### Etapa 0 — Data Wrangling (`data_wrangling.py`)

**Pergunta:** Os dados brutos estão confiáveis para modelagem?

**Input:**
- `db_base_processos.csv` — 27.783 processos históricos (2023-2025) com datas, modal, serviço e valor em formato brasileiro
- `db_metas.csv` — 48 metas mensais de faturamento para 2026 (4 serviços × 12 meses)

**O que faz:**
1. Converte valores BR (`1.234,56`) e datas (`DD/MM/AAAA`) para tipos nativos
2. Remove nulos em campos essenciais (processo, datas, modal, serviço, valor)
3. Calcula `lead_time = dt_faturamento - dt_abertura` e descarta processos com lead_time fora de [0, 365] dias
4. Padroniza modais: `AIRFREIGHT → Aéreo`, `OCEANFREIGHT/FCL/LCL/BREAK BULK → Marítimo`, `RODOVIARIO → Rodoviário`
5. Remove outliers de valor usando IQR **por combinação modal×serviço** (não global — preserva processos legítimos de alto valor em segmentos como Trading)
6. Deriva campos de mês/ano de abertura e faturamento

**Output:** `df` com **25.284 processos limpos** (91% de aproveitamento) + `df_metas` com 48 metas validadas.

**CSVs gerados:** nenhum (passo intermediário).

---

### Etapa 1 — Análise Histórica (`analise_historica.py`)

**Pergunta:** Quais são os parâmetros estruturais do negócio que o modelo vai usar?

**Input:** `df` limpo da Etapa 0.

**O que faz:**
1. **Lead time por modal** — mediana, média, desvio (Marítimo: 77 dias, Aéreo: 23, Rodoviário: 17). Mediana é usada como referência por ser robusta a outliers.
2. **Share de modal por serviço** (por faturamento) — ex.: Trading é 77,8% Marítimo, 10,3% Aéreo, 11,9% Rodoviário
3. **Ticket médio por combinação modal×serviço** — 11 combinações válidas (mínimo 10 processos por combinação). Maior: Trading Marítimo (R$ 5.178); menor: Seguro Aéreo (R$ 390)
4. **Ticket médio por modal** — fallback caso alguma combinação não tenha amostra suficiente

**Output:** dicionário `metricas` com 4 chaves (`lead_times`, `share_modal_por_servico`, `ticket_modal`, `ticket_modal_servico`).

**CSVs gerados:**
- `lead_times.csv` → **Tabela 1 do TCC**
- `share_modal_por_servico.csv` → referência textual §5.3
- `ticket_medio_modal_servico.csv` → **Tabela 2 do TCC**

---

### Etapa 2 — Distribuição Temporal (`distribuicao_temporal.py`)

**Pergunta:** Em **qual mês** os processos precisam ser abertos para faturar no mês-meta?

**Input:** `df_metas` + `metricas` da Etapa 1.

**O que faz:** Para cada uma das 48 metas (mês × serviço):

1. **Distribui a meta entre modais** usando o share histórico. Ex.: meta de Trading em jan/2026 = R$ 1.500.000 vira:
   - Trading Marítimo (77,8%): R$ 1.166.700
   - Trading Aéreo (10,3%): R$ 154.350
   - Trading Rodoviário (11,9%): R$ 178.950

2. **Calcula a data limite de abertura** subtraindo o lead time do modal da data-meta. Ex.: Trading Marítimo para faturar em 01/jan/2026 → `01/jan/2026 − 77 dias = 16/out/2025` → **mês de abertura: out/2025**.

**Output:** `df_dist` com **132 combinações** (mês-meta × serviço × modal, pulando combinações com share = 0 como Agenciamento Rodoviário).

**CSVs gerados:** nenhum (passo intermediário).

---

### Etapa 3 — Meta Reversa (`meta_reversa.py`) — NÚCLEO DO MODELO

**Pergunta:** Finalmente — **quantos processos** abrir em cada mês, por modal e serviço?

**Input:** `df_dist` (132 combinações) + `metricas`.

**O que faz:** Para cada combinação, aplica a fórmula fundamental:

```
P = ⌈ MF / TM ⌉
```

onde:
- `P` = número de processos necessários
- `MF` = meta financeira da combinação (modal × serviço × mês)
- `TM` = ticket médio histórico (específico se tiver amostra ≥10, senão fallback por modal)
- `⌈ ⌉` = ceiling, garante que `P × TM ≥ MF` (sempre cobre a meta, nunca sub-recomenda)

Consolida em duas perspectivas:
- **Por mês de abertura** (operacional — "o que fazer em cada mês")
- **Por mês-meta** (financeiro — "o que fatura em cada mês")

**Output:** 3 DataFrames:
- `df_rec` — 132 recomendações detalhadas (com ticket, fonte, data limite precisa)
- `tabela_aberturas` — pivô por mês de abertura × (serviço, modal)
- `tabela_faturamento` — pivô por mês-meta × (serviço, modal)

**Resultado final:** **20.139 processos** distribuídos em 14 meses (out/2025 a nov/2026) para atingir a meta anual de R$ 46,25 milhões.

**CSVs gerados:**
- `tabela_aberturas.csv` → **Tabela 3 do TCC** (entregável principal)
- `processos_por_mes_meta.csv` → **Tabela 4 do TCC**
- `recomendacoes_detalhadas.csv` → anexo de auditoria com rastreabilidade

---

### Etapa 4 — Backtesting (`backtesting.py`)

**Pergunta:** O modelo funciona? Se tivéssemos rodado ele no passado, teria acertado?

**Input:** `df` (dados históricos completos).

**O que faz:** Simula a aplicação retrospectiva do modelo em **dois splits temporais**:

- **Split A (robustez):** treino 2023 → teste 2024
- **Split B (principal):** treino 2023+2024 → teste 2025

Em cada split:
1. Recalcula todas as métricas (share, ticket) **apenas com dados de treino** — sem data leakage
2. Usa o faturamento real do ano de teste como "metas ex-post" (o que teríamos recebido como meta se aplicássemos o modelo prospectivamente)
3. Aplica o **modelo reverso completo** sobre essas metas
4. Compara os processos recomendados com os efetivamente abertos no ano de teste
5. Calcula MAPE, BIAS e HIT RATE

**Output:**

| Split | MAPE | BIAS | HIT RATE |
|---|---|---|---|
| 2023 → 2024 | 8,15% | +8,15% | 91,67% |
| 2023+2024 → 2025 | 10,80% | +10,80% | 100% |

BIAS positivo = modelo super-recomenda por design (efeito do ceiling). MAPE ≡ BIAS por invariante estrutural (todos os erros são positivos — o modelo nunca sub-recomenda).

**CSVs gerados:**
- `backtesting_mensal.csv` → **Tabela 5 do TCC** (detalhamento mensal dos dois splits)
- `performance_backtesting.csv` → **Tabela 6 do TCC**

---

### Etapa 5 — Análise de Sensibilidade (`analise_sensibilidade.py`)

**Pergunta:** Se o ticket médio ou o lead time mudarem, como a recomendação se ajusta?

**Input:** `df_metas` + `metricas`.

**O que faz:**

1. **Sensibilidade ao ticket** — reroda o modelo com 5 cenários: −20%, −10%, base, +10%, +20%
2. **Sensibilidade ao lead time** — calcula o deslocamento em dias da data de abertura em 5 cenários: −50%, −20%, base, +20%, +50%

**Resultados-chave:**
- **Ticket −20% → +24,9% processos** / Ticket +20% → −16,6% processos (assimetria matemática do cálculo reverso)
- **Marítimo ±20% lead time → ±15 dias** na data de abertura (modal mais crítico)

**CSVs gerados:**
- `sensibilidade_ticket.csv` → **Tabela 7 do TCC**
- `sensibilidade_lead_time.csv` → **Tabela 8 do TCC**

---

## Entregáveis (`outputs/`)

| Arquivo | Descrição |
|---------|-----------|
| `tabela_aberturas.csv` | **Entregável principal** — processos a abrir por mês/modal/serviço |
| `processos_por_mes_meta.csv` | Processos por mês-meta (quando faturar) |
| `recomendacoes_detalhadas.csv` | Detalhamento com ticket, `ticket_fonte` (específico/fallback) e valor esperado |
| `backtesting_mensal.csv` | Série mensal consolidada dos dois splits (processos reais vs. recomendados) |
| `performance_backtesting.csv` | Métricas por split: MAPE, BIAS e HIT RATE |
| `sensibilidade_ticket.csv` | Impacto de variações no ticket médio |
| `sensibilidade_lead_time.csv` | Impacto de variações no lead time |
| `lead_times.csv` | Lead times históricos por modal |
| `ticket_medio_modal_servico.csv` | Ticket médio por combinação |
| `share_modal_por_servico.csv` | Distribuição de modais por serviço |

---

## O que o projeto entrega na prática

Para o gestor comercial, o entregável principal (`tabela_aberturas.csv`) responde de forma concreta:

> *"Para atingir a meta de R$ 46,25 milhões em 2026, abram exatamente esses números de processos em cada mês, por modal e serviço, começando em outubro de 2025."*

Os outros outputs garantem:
- **Confiança metodológica** — backtesting em 2 splits com HIT RATE ≥ 91,7% e erro máximo mensal abaixo do limite de 20%
- **Flexibilidade para cenários** — análise de sensibilidade quantifica impacto de variações
- **Rastreabilidade** — recomendações detalhadas com fonte do ticket e data precisa de abertura

O pipeline transforma uma **meta financeira abstrata** em um **cronograma operacional acionável**, com validação estatística rigorosa e parâmetros auditáveis.

---

## Dependências

- `pandas >= 2.2, < 3.0`
