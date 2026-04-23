"""
Modelo de Meta Reversa para Planejamento de Aberturas em Operações de Importação
Autor: Pedro Henrique Scholles
MBA em Data Science e Analytics - USP/Esalq
"""

import os
import pandas as pd
from src import (
    data_wrangling,
    analise_historica,
    distribuicao_temporal,
    meta_reversa,
    backtesting,
    analise_sensibilidade,
)

ARQUIVO_PROCESSOS = 'data/db_base_processos.csv'
ARQUIVO_METAS     = 'data/db_metas.csv'
DIR_OUTPUTS       = 'outputs'


def _salvar_outputs(metricas, df_rec, tabela_aberturas, tabela_faturamento,
                    resultado_backtest, df_sens_ticket, df_sens_lead):
    """Persiste todos os resultados do pipeline em arquivos CSV na pasta outputs/."""
    os.makedirs(DIR_OUTPUTS, exist_ok=True)

    # --- Lead times ---
    pd.DataFrame(metricas['lead_times']).T.to_csv(
        f'{DIR_OUTPUTS}/lead_times.csv', encoding='utf-8-sig'
    )

    # --- Ticket médio por modal + serviço ---
    rows = [
        {'modal': k[0], 'servico': k[1], 'ticket_medio': v}
        for k, v in metricas['ticket_modal_servico'].items()
    ]
    pd.DataFrame(rows).sort_values('ticket_medio', ascending=False).to_csv(
        f'{DIR_OUTPUTS}/ticket_medio_modal_servico.csv', index=False, encoding='utf-8-sig'
    )

    # --- Share geral por modal ---
    share_geral_rows = [
        {'modal': modal, 'share_pct': pct}
        for modal, pct in metricas['share_geral_modal'].items()
    ]
    pd.DataFrame(share_geral_rows).to_csv(
        f'{DIR_OUTPUTS}/share_geral_modal.csv', index=False, encoding='utf-8-sig'
    )

    # --- Share de modal por serviço ---
    share_rows = []
    for servico, shares in metricas['share_modal_por_servico'].items():
        for modal, pct in shares.items():
            share_rows.append({'servico': servico, 'modal': modal, 'share_pct': pct})
    pd.DataFrame(share_rows).to_csv(
        f'{DIR_OUTPUTS}/share_modal_por_servico.csv', index=False, encoding='utf-8-sig'
    )

    # --- Tabela de aberturas (Tabela 3 do TCC) ---
    tabela_aberturas.to_csv(
        f'{DIR_OUTPUTS}/tabela_aberturas.csv', encoding='utf-8-sig'
    )

    # --- Processos necessários por mês-meta (Tabela 4 do TCC) ---
    tabela_faturamento.to_csv(
        f'{DIR_OUTPUTS}/processos_por_mes_meta.csv', encoding='utf-8-sig'
    )

    # --- Recomendações detalhadas ---
    df_rec.to_csv(
        f'{DIR_OUTPUTS}/recomendacoes_detalhadas.csv', index=False, encoding='utf-8-sig'
    )

    # --- Backtesting: série mensal consolidada dos dois splits ---
    r1, r2 = resultado_backtest
    backtesting_mensal = pd.concat(
        [r['agregado'] for r in (r1, r2) if r is not None],
        ignore_index=True
    )
    backtesting_mensal.to_csv(
        f'{DIR_OUTPUTS}/backtesting_mensal.csv', index=False, encoding='utf-8-sig'
    )

    # --- Performance consolidada dos dois splits ---
    perf_rows = []
    for r in (r1, r2):
        if r is None:
            continue
        m = r['metricas']
        perf_rows.append({
            'split':         r['split'],
            'ano_teste':     r['ano_teste'],
            'MAPE (%)':      round(m['mape'], 2),
            'BIAS (%)':      round(m['bias'], 2),
            'HIT_RATE (%)':  round(m['hit_rate'], 2),
            'n_meses':       m['n'],
        })
    pd.DataFrame(perf_rows).to_csv(
        f'{DIR_OUTPUTS}/performance_backtesting.csv', index=False, encoding='utf-8-sig'
    )

    # --- Análise de sensibilidade ---
    df_sens_ticket.to_csv(
        f'{DIR_OUTPUTS}/sensibilidade_ticket.csv', index=False, encoding='utf-8-sig'
    )
    df_sens_lead.to_csv(
        f'{DIR_OUTPUTS}/sensibilidade_lead_time.csv', index=False, encoding='utf-8-sig'
    )

    print("\n" + "="*80)
    print("OUTPUTS SALVOS EM: outputs/")
    print("="*80)
    for fname in sorted(os.listdir(DIR_OUTPUTS)):
        fpath = os.path.join(DIR_OUTPUTS, fname)
        size  = os.path.getsize(fpath)
        print(f"  {fname:<45} {size:>8,} bytes")


def executar_pipeline():
    print("=" * 80)
    print("MODELO DE META REVERSA - PIPELINE COMPLETO")
    print("=" * 80)

    # Etapa 0: ETL
    df, df_metas = data_wrangling.executar(ARQUIVO_PROCESSOS, ARQUIVO_METAS)

    # Etapa 1: Métricas históricas (dados completos 2023-2025, usadas para planejamento 2026)
    metricas = analise_historica.executar(df)

    # Etapa 2: Distribuição temporal de aberturas
    df_dist = distribuicao_temporal.executar(df_metas, metricas)

    # Etapa 3: Cálculo de meta reversa
    df_rec, tabela_aberturas, tabela_faturamento = meta_reversa.executar(df_dist, metricas)

    # Etapa 4: Backtesting out-of-sample (simulação reversa com metas ex-post; dois splits)
    resultado_backtest = backtesting.executar(df)

    # Etapa 5: Análise de sensibilidade
    df_sens_ticket, df_sens_lead = analise_sensibilidade.executar(df_metas, metricas)

    # Salvar todos os outputs
    _salvar_outputs(metricas, df_rec, tabela_aberturas, tabela_faturamento,
                    resultado_backtest, df_sens_ticket, df_sens_lead)

    print("\n" + "=" * 80)
    print("PIPELINE CONCLUIDO")
    print("=" * 80)

    return {
        'df_processos':         df,
        'df_metas':             df_metas,
        'metricas':             metricas,
        'distribuicao':         df_dist,
        'recomendacoes':        df_rec,
        'tabela_aberturas':     tabela_aberturas,
        'tabela_faturamento':   tabela_faturamento,
        'backtest':             resultado_backtest,
        'sensibilidade_ticket': df_sens_ticket,
        'sensibilidade_lead':   df_sens_lead,
    }


if __name__ == "__main__":
    executar_pipeline()