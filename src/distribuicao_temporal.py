"""
Etapa 2: Distribuição Temporal de Aberturas
Define quando abrir processos para cada combinação (mês, serviço, modal).
"""

import pandas as pd
from datetime import datetime, timedelta


def executar(df_metas, metricas):
    print("\n" + "="*80)
    print("ETAPA 2: DISTRIBUIÇÃO TEMPORAL DE ABERTURAS")
    print("="*80)

    print(f"\nMetas carregadas: {len(df_metas)} registros")
    print(f"Meta total anual: R$ {df_metas['vlr_meta'].sum():,.2f}")

    distribuicao = []

    for _, meta_row in df_metas.iterrows():
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

            lead_time = metricas['lead_times'].get(modal, {}).get('mediano')
            if lead_time is None:
                print(f"  AVISO: sem lead time para modal {modal} - combinacao ignorada")
                continue
            meta_modal_servico    = meta_servico * share_modal
            data_abertura_limite  = data_meta - timedelta(days=lead_time)

            distribuicao.append({
                'mes_meta':             mes_meta,
                'ano_meta':             ano_meta,
                'servico':              servico,
                'meta_servico':         meta_servico,
                'modal':                modal,
                'share_modal':          share_modal * 100,
                'meta_modal_servico':   meta_modal_servico,
                'lead_time':            lead_time,
                'data_abertura_limite': data_abertura_limite,
                'mes_abertura':         data_abertura_limite.month,
                'ano_abertura':         data_abertura_limite.year
            })

    df_dist = pd.DataFrame(distribuicao)
    print(f"\nDistribuição calculada: {len(df_dist)} combinações (mês x serviço x modal)")

    esperado = 132
    if abs(len(df_dist) - esperado) > esperado * 0.1:
        print(f"  AVISO: esperado ~{esperado} combinações, gerado {len(df_dist)}. "
              f"Verifique shares e lead times históricos.")

    return df_dist
