"""
Etapa 2: Distribuição Temporal de Aberturas
Define quando abrir processos para cada combinação (mês, serviço, modal).
"""

import pandas as pd
from datetime import datetime, timedelta


def executar(df_metas_raw, metricas, data_ref=None):
    print("\n" + "="*80)
    print("ETAPA 2: DISTRIBUIÇÃO TEMPORAL DE ABERTURAS")
    print("="*80)

    if data_ref is None:
        data_ref = datetime.now()

    print(f"Data de referência: {data_ref.date()}")

    df_metas_raw['mes'] = df_metas_raw['mes'].astype(int)
    df_metas_raw['ano'] = df_metas_raw['ano'].astype(int)

    print(f"\nMetas carregadas: {len(df_metas_raw)} registros")
    print(f"Meta total anual: R$ {df_metas_raw['vlr_meta'].sum():,.2f}")

    distribuicao = []

    for _, meta_row in df_metas_raw.iterrows():
        mes_meta     = meta_row['mes']
        ano_meta     = meta_row['ano']
        servico      = meta_row['servico']
        meta_servico = meta_row['vlr_meta']

        data_meta     = datetime(ano_meta, mes_meta, 1)
        share_servico = metricas['share_modal_por_servico'].get(servico, {})

        for modal in ['Marítimo', 'Aéreo', 'Rodoviário']:
            share_modal = share_servico.get(modal, 0) / 100

            if share_modal == 0:
                continue

            lead_time          = metricas['lead_times'][modal]['mediano']
            meta_modal_servico = meta_servico * share_modal
            data_limite        = data_meta - timedelta(days=lead_time)

            distribuicao.append({
                'mes_meta':           mes_meta,
                'ano_meta':           ano_meta,
                'servico':            servico,
                'meta_servico':       meta_servico,
                'modal':              modal,
                'share_modal':        share_modal * 100,
                'meta_modal_servico': meta_modal_servico,
                'lead_time':          lead_time,
                'data_limite':        data_limite,
                'mes_abertura':       data_limite.month,
                'ano_abertura':       data_limite.year
            })

    df_dist = pd.DataFrame(distribuicao)
    print(f"\nDistribuição calculada: {len(df_dist)} combinações (mês x serviço x modal)")

    return df_dist
