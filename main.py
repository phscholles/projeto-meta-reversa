"""
Modelo de Meta Reversa para Planejamento de Aberturas em Operações de Importação
Autor: Pedro Henrique Scholles
MBA em Data Science e Analytics - USP/Esalq
"""

import os
import pandas as pd
from datetime import datetime
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
DATA_REFERENCIA   = datetime(2026, 2, 10)
DIR_OUTPUTS       = 'outputs'


def _salvar_outputs(metricas, df_rec, tabela_aberturas, fat_real, fat_previsto,
                    performance, df_sens_ticket, df_sens_lead):
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

    # --- Share de modal por serviço ---
    share_rows = []
    for servico, shares in metricas['share_modal_por_servico'].items():
        for modal, pct in shares.items():
            share_rows.append({'servico': servico, 'modal': modal, 'share_pct': pct})
    pd.DataFrame(share_rows).to_csv(
        f'{DIR_OUTPUTS}/share_modal_por_servico.csv', index=False, encoding='utf-8-sig'
    )

    # --- Tabela de aberturas (Tabela 1 do TCC) ---
    tabela_aberturas.to_csv(
        f'{DIR_OUTPUTS}/tabela_aberturas.csv', encoding='utf-8-sig'
    )

    # --- Recomendações detalhadas ---
    df_rec.to_csv(
        f'{DIR_OUTPUTS}/recomendacoes_detalhadas.csv', index=False, encoding='utf-8-sig'
    )

    # --- Backtesting mensal (Tabela 3 do TCC) ---
    performance['df_detalhado'].to_csv(
        f'{DIR_OUTPUTS}/backtesting_mensal.csv', index=False, encoding='utf-8-sig'
    )

    # --- Métricas de performance ---
    pd.DataFrame([{
        'MAPE (%)':     round(performance['mape'], 2),
        'BIAS (%)':     round(performance['bias'], 2),
        'HIT_RATE (%)': round(performance['hit_rate'], 2),
    }]).to_csv(
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
    df_dist = distribuicao_temporal.executar(df_metas, metricas, DATA_REFERENCIA)

    # Etapa 3: Cálculo de meta reversa
    df_rec, tabela_aberturas = meta_reversa.executar(df_dist, metricas)

    # Etapa 4: Backtesting out-of-sample (métricas recalculadas internamente só com 2023-2024)
    fat_real, fat_previsto, performance = backtesting.executar(df, metricas)

    # Etapa 5: Análise de sensibilidade
    df_sens_ticket, df_sens_lead = analise_sensibilidade.executar(df_metas, metricas)

    # Salvar todos os outputs
    _salvar_outputs(metricas, df_rec, tabela_aberturas, fat_real, fat_previsto,
                    performance, df_sens_ticket, df_sens_lead)

    print("\n" + "=" * 80)
    print("PIPELINE CONCLUIDO")
    print("=" * 80)

    return {
        'df_processos':      df,
        'df_metas':          df_metas,
        'metricas':          metricas,
        'distribuicao':      df_dist,
        'recomendacoes':     df_rec,
        'tabela_aberturas':  tabela_aberturas,
        'faturamento_real':  fat_real,
        'fat_previsto':      fat_previsto,
        'performance':       performance,
        'sensibilidade_ticket': df_sens_ticket,
        'sensibilidade_lead':   df_sens_lead,
    }


if __name__ == "__main__":
    executar_pipeline()
