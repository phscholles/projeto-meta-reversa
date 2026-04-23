"""
Analise Diagnostica: Decomposicao do BIAS do Backtesting

Modulo auxiliar que isola as fontes do vies positivo observado no Algoritmo 4
(Backtesting). O BIAS +10,8% reportado no 'split' principal (2025) decompoe-se
em duas fontes estruturais:

  1. Operador 'ceiling': arredondamento para cima de processos fracionarios,
     que por construcao produz vies positivo (nunca sub-recomenda). Seu
     componente e isolado por experimento com operadores alternativos
     (ceil vs. round vs. floor), comparando o BIAS do ceiling com o BIAS
     do operador piso (floor) - que por definicao nao contribui com vies
     de arredondamento positivo.
  2. Evolucao do ticket medio entre treino e teste: aumento do ticket no
     periodo de teste faz com que o modelo, calibrado com ticket historico
     mais baixo, recomende mais processos do que o efetivamente necessario.

Metodologia da decomposicao (alinhada ao reportado no TCC):
  - Componente ceiling = BIAS(ceil) - BIAS(floor)
    (amplitude entre o operador de producao e o operador piso, quantificando
     o quanto o arredondamento para cima adiciona em relacao ao piso)
  - Componente ticket = BIAS(floor)
    (o vies que permanece mesmo com operador piso, atribuivel apenas a
     evolucao do ticket medio entre treino e teste)
"""

import math
import pandas as pd
from src import analise_historica


def _aplicar_modelo_reverso(metas_df, metricas_treino, operador='ceil'):
    """Aplica share + ticket sobre metas, usando o operador de arredondamento especificado.

    Args:
        operador: 'ceil' (producao), 'round' (neutro), 'floor' (piso).
    """
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

            valor_fracionario = meta_modal / ticket
            if operador == 'ceil':
                processos = math.ceil(valor_fracionario)
            elif operador == 'round':
                processos = round(valor_fracionario)
            elif operador == 'floor':
                processos = math.floor(valor_fracionario)
            else:
                raise ValueError(f"Operador desconhecido: {operador}")

            recomendacoes.append({
                'mes':                    mes,
                'servico':                servico,
                'modal':                  modal,
                'processos_recomendados': processos,
            })
    return pd.DataFrame(recomendacoes)


def _bias_agregado(df_rec, df_teste):
    """Calcula o BIAS mensal medio (processos recomendados vs reais)."""
    processos_reais = (
        df_teste.groupby(['mes_faturamento', 'modal', 'servico'])
        .agg(processos_reais=('processo', 'count'))
        .reset_index()
        .rename(columns={'mes_faturamento': 'mes'})
    )

    df_cmp = pd.merge(
        df_rec, processos_reais, on=['mes', 'modal', 'servico'], how='outer'
    ).fillna({'processos_recomendados': 0, 'processos_reais': 0})

    agregado = df_cmp.groupby('mes').agg(
        processos_reais=('processos_reais', 'sum'),
        processos_recomendados=('processos_recomendados', 'sum'),
    ).reset_index()

    erro = (
        (agregado['processos_recomendados'] - agregado['processos_reais'])
        / agregado['processos_reais'].where(agregado['processos_reais'] > 0) * 100
    ).dropna()

    return erro.mean()


def decompor(df, ano_teste=2025):
    """
    Executa a decomposicao do BIAS para o split principal via experimento
    com operadores alternativos (ceil vs. floor), conforme metodologia
    reportada no TCC.

    Args:
        df: base completa de processos (saida do data_wrangling).
        ano_teste: ano usado como conjunto de teste (default 2025).

    Returns:
        dict com os tres operadores testados e os componentes da decomposicao.
    """
    print("\n" + "="*80)
    print(f"DECOMPOSICAO DO BIAS - Split: Treino < {ano_teste} / Teste: {ano_teste}")
    print("Metodologia: experimento com operadores alternativos (ceil/round/floor)")
    print("="*80)

    df_treino = df[df['ano_faturamento'] < ano_teste].copy()
    df_teste  = df[df['ano_faturamento'] == ano_teste].copy()

    # Metricas estritamente out-of-sample
    metricas_treino = analise_historica.executar(df_treino, verbose=False)

    # Metas ex-post: faturamento real do ano de teste
    metas_ex_post = (
        df_teste.groupby(['mes_faturamento', 'servico'])['vlr_faturamento']
        .sum()
        .reset_index()
        .rename(columns={'mes_faturamento': 'mes', 'vlr_faturamento': 'vlr_meta'})
    )

    # Rodar os tres cenarios
    bias_por_operador = {}
    for operador in ('ceil', 'round', 'floor'):
        df_rec = _aplicar_modelo_reverso(metas_ex_post, metricas_treino, operador=operador)
        bias = _bias_agregado(df_rec, df_teste)
        bias_por_operador[operador] = bias

    bias_ceil  = bias_por_operador['ceil']
    bias_round = bias_por_operador['round']
    bias_floor = bias_por_operador['floor']

    # Decomposicao (alinhada ao reportado no TCC):
    #   BIAS total (ceil) = componente do ceiling + componente do ticket
    #   componente do ticket  = BIAS(floor)  - vies residual mesmo com operador piso
    #   componente do ceiling = BIAS(ceil) - BIAS(floor)
    componente_ticket  = bias_floor
    componente_ceiling = bias_ceil - bias_floor

    print(f"\n--- BIAS por operador ---")
    print(f"  ceil  (producao)     : {bias_ceil:+.2f} pp")
    print(f"  round (neutro)       : {bias_round:+.2f} pp")
    print(f"  floor (piso)         : {bias_floor:+.2f} pp")

    print(f"\n--- DECOMPOSICAO ---")
    print(f"  BIAS TOTAL (ceil)               : {bias_ceil:+.2f} pp")
    print(f"  Componente da EVOLUCAO DO TICKET: {componente_ticket:+.2f} pp")
    print(f"    (BIAS com operador piso 'floor')")
    print(f"  Componente do CEILING           : {componente_ceiling:+.2f} pp")
    print(f"    (diferenca BIAS[ceil] - BIAS[floor])")
    print(f"  SOMA (verificacao = BIAS total) : {componente_ceiling + componente_ticket:+.2f} pp")

    print(f"\n--- INTERPRETACAO ---")
    print(f"  Do BIAS de {bias_ceil:+.2f} pp observado no split {ano_teste}:")
    print(f"    - {componente_ceiling:.2f} pp sao atribuiveis ao operador ceiling")
    print(f"      (vies de arredondamento para cima em relacao ao piso).")
    print(f"    - {componente_ticket:.2f} pp decorrem da evolucao do ticket medio")
    print(f"      entre o periodo de treino e o periodo de teste.")

    return {
        'ano_teste':                  ano_teste,
        'bias_ceil_pp':               round(bias_ceil, 2),
        'bias_round_pp':              round(bias_round, 2),
        'bias_floor_pp':              round(bias_floor, 2),
        'componente_ceiling_pp':      round(componente_ceiling, 2),
        'componente_ticket_pp':       round(componente_ticket, 2),
    }


if __name__ == "__main__":
    """Execucao standalone para diagnostico."""
    from src import data_wrangling

    df, _ = data_wrangling.executar(
        'data/db_base_processos.csv',
        'data/db_metas.csv'
    )

    resultado = decompor(df, ano_teste=2025)

    # Salva resultado em CSV
    import os
    os.makedirs('outputs', exist_ok=True)
    pd.DataFrame([resultado]).to_csv(
        'outputs/decomposicao_bias.csv', index=False, encoding='utf-8-sig'
    )
    print(f"\nResultado salvo em: outputs/decomposicao_bias.csv")