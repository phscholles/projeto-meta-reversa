"""
Etapa 4: Backtesting Out-of-Sample — Simulação Reversa com Metas Ex-Post.

Valida o modelo completo (share + ticket + ceiling) aplicando-o retrospectivamente
sobre metas ex-post conhecidas. Todas as métricas (ticket, share) vêm apenas do
conjunto de treino — sem data leakage.

Métricas apuradas sobre o número de processos recomendados vs. efetivamente abertos:
  - MAPE: erro percentual médio absoluto
  - BIAS: viés sistemático (com sinal)
  - HIT RATE: % de meses com erro dentro de ±20%

Executa dois splits para demonstrar robustez:
  - 2023 -> teste 2024
  - 2023+2024 -> teste 2025
"""

import math
import pandas as pd
from src import analise_historica


def _aplicar_modelo_reverso(metas_df, metricas_treino):
    """Aplica share + ticket + ceiling sobre um conjunto de metas (mes, servico, valor)."""
    recomendacoes = []
    share_por_servico    = metricas_treino['share_modal_por_servico']
    ticket_modal_servico = metricas_treino['ticket_modal_servico']
    ticket_modal         = metricas_treino['ticket_modal']

    for _, row in metas_df.iterrows():
        mes     = row['mes']
        servico = row['servico']
        meta    = row['vlr_meta']
        shares  = share_por_servico.get(servico, {})

        for modal, share_pct in shares.items():
            share = share_pct / 100
            if share == 0:
                continue
            meta_modal = meta * share

            ticket = ticket_modal_servico.get((modal, servico))
            if ticket is None or ticket == 0:
                ticket = ticket_modal.get(modal, 0)
            if ticket == 0:
                continue

            processos = int(math.ceil(meta_modal / ticket))
            recomendacoes.append({
                'mes':                    mes,
                'servico':                servico,
                'modal':                  modal,
                'processos_recomendados': processos,
            })
    return pd.DataFrame(recomendacoes)


def _metricas(df_agg, col_real, col_prev, banda=20):
    erro = (
        (df_agg[col_prev] - df_agg[col_real])
        / df_agg[col_real].where(df_agg[col_real] > 0) * 100
    ).dropna()
    n = len(erro)
    if n == 0:
        return {'mape': float('nan'), 'bias': float('nan'), 'hit_rate': float('nan'), 'n': 0}
    return {
        'mape':     erro.abs().mean(),
        'bias':     erro.mean(),
        'hit_rate': (erro.abs() <= banda).sum() / n * 100,
        'n':        n,
    }


def _rodar_split(df, ano_teste, label):
    print("\n" + "="*80)
    print(f"SPLIT: {label}")
    print("="*80)

    df_treino = df[df['ano_faturamento'] < ano_teste].copy()
    df_teste  = df[df['ano_faturamento'] == ano_teste].copy()
    print(f"Treino: {len(df_treino):,} processos (faturados antes de {ano_teste})")
    print(f"Teste : {len(df_teste):,} processos (faturados em {ano_teste})")

    if len(df_treino) == 0 or len(df_teste) == 0:
        print("AVISO: split sem dados suficientes, pulando.")
        return None

    # Métricas estritamente out-of-sample
    metricas_treino = analise_historica.executar(df_treino, verbose=False)

    # "Metas ex-post": o faturamento real de cada mês do ano de teste por serviço.
    # Representa o que teríamos recebido como meta se o modelo fosse aplicado
    # prospectivamente ao fim do ano anterior.
    metas_ex_post = (
        df_teste.groupby(['mes_faturamento', 'servico'])['vlr_faturamento']
        .sum()
        .reset_index()
        .rename(columns={'mes_faturamento': 'mes', 'vlr_faturamento': 'vlr_meta'})
    )

    # Aplicar o modelo reverso COMPLETO (share + ticket + ceiling)
    df_rec = _aplicar_modelo_reverso(metas_ex_post, metricas_treino)

    # Alvo: processos efetivamente abertos que foram faturados em cada (mes, modal, servico)
    processos_reais = (
        df_teste.groupby(['mes_faturamento', 'modal', 'servico'])
        .agg(processos_reais=('processo', 'count'))
        .reset_index()
        .rename(columns={'mes_faturamento': 'mes'})
    )

    df_cmp = pd.merge(
        df_rec, processos_reais, on=['mes', 'modal', 'servico'], how='outer'
    ).fillna({'processos_recomendados': 0, 'processos_reais': 0})
    df_cmp[['processos_recomendados', 'processos_reais']] = (
        df_cmp[['processos_recomendados', 'processos_reais']].astype(int)
    )

    agregado = df_cmp.groupby('mes').agg(
        processos_reais=('processos_reais', 'sum'),
        processos_recomendados=('processos_recomendados', 'sum'),
    ).reset_index()

    agregado['erro_pct'] = (
        (agregado['processos_recomendados'] - agregado['processos_reais'])
        / agregado['processos_reais'].where(agregado['processos_reais'] > 0) * 100
    )

    met = _metricas(agregado, 'processos_reais', 'processos_recomendados')

    print(f"\n--- METRICAS ---")
    print(f"  MAPE    : {met['mape']:.1f}%  [meta: < 20%]")
    print(f"  BIAS    : {met['bias']:+.1f}%  [+ super-recomenda, - sub-recomenda]")
    print(f"  HIT RATE: {met['hit_rate']:.1f}%  [meta: >= 80%]")

    print(f"\nDetalhamento mensal:")
    print(f"{'Mes':>4} {'ProcReal':>9} {'ProcRec':>9} {'Err%':>8}")
    print("-" * 40)
    for _, r in agregado.iterrows():
        s = f"{r['erro_pct']:+6.1f}%" if pd.notna(r['erro_pct']) else "   n/d"
        print(f"  {int(r['mes']):02d}  {int(r['processos_reais']):>8,} {int(r['processos_recomendados']):>8,}  {s}")

    tot_pr = int(agregado['processos_reais'].sum())
    tot_pc = int(agregado['processos_recomendados'].sum())
    e_pr   = (tot_pc - tot_pr) / tot_pr * 100 if tot_pr > 0 else float('nan')
    print(f"\n  TOT {tot_pr:>8,} {tot_pc:>8,}  {e_pr:+6.1f}%")

    # Tag do split para identificação nos CSVs quando consolidado
    agregado.insert(0, 'split', label)

    return {
        'split':     label,
        'ano_teste': ano_teste,
        'metricas':  met,
        'agregado':  agregado,
    }


def executar(df):
    """Executa backtesting com dois splits para validação de robustez."""
    print("\n" + "="*80)
    print("ETAPA 4: BACKTESTING OUT-OF-SAMPLE — SIMULACAO REVERSA COM METAS EX-POST")
    print("="*80)
    print("\nMetodologia:")
    print("  1. Separacao temporal rigorosa (treino/teste)")
    print("  2. Metricas (share, ticket) calculadas SO com treino")
    print("  3. Faturamento real do ano de teste vira 'metas ex-post'")
    print("  4. Modelo reverso completo (share+ticket+ceiling) aplicado sobre as metas")
    print("  5. Processos recomendados comparados com processos efetivamente abertos")

    r1 = _rodar_split(df, 2024, 'Treino: 2023 -> Teste: 2024')
    r2 = _rodar_split(df, 2025, 'Treino: 2023+2024 -> Teste: 2025')

    print("\n" + "="*80)
    print("RESUMO CONSOLIDADO")
    print("="*80)
    print(f"\n{'Split':<36} {'MAPE':>8} {'BIAS':>9} {'HIT_RATE':>10}")
    print("-" * 65)
    for r in (r1, r2):
        if r is None:
            continue
        m = r['metricas']
        print(f"  {r['split']:<34} {m['mape']:>6.1f}%  {m['bias']:>+7.1f}%  {m['hit_rate']:>8.1f}%")

    return r1, r2
