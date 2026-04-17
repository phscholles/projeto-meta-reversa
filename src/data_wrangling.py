"""
Etapa 0: Data Wrangling
Carrega, limpa e prepara os dados para análise.
"""

import pandas as pd


def executar(arquivo_processos, arquivo_metas):
    print("\n" + "="*80)
    print("ETAPA 0: DATA WRANGLING")
    print("="*80)

    # Carregar base de processos
    df = pd.read_csv(arquivo_processos, sep=';', encoding='utf-8-sig')
    n_original = len(df)
    print(f"Processos carregados: {n_original:,}")
    if n_original == 0:
        raise ValueError(f"Base de processos vazia: {arquivo_processos}")

    # Converter valores brasileiros (vírgula → ponto decimal).
    # Assume formato BR puro (ex: "2.636,97"). Valores em formato US ("2636.97")
    # seriam corrompidos — revisar caso o layout da fonte mude.
    df['vlr_faturamento'] = (
        df['vlr_faturamento']
        .astype(str)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
    )
    df['vlr_faturamento'] = pd.to_numeric(df['vlr_faturamento'], errors='coerce')

    # Converter datas
    df['dt_abertura'] = pd.to_datetime(df['dt_abertura'], format='%d/%m/%Y', errors='coerce')
    df['dt_faturamento'] = pd.to_datetime(df['dt_faturamento'], format='%d/%m/%Y', errors='coerce')

    # Remover nulos essenciais e modal vazio antes de derivar lead_time
    df = df[df['modal'].notna() & (df['modal'].str.strip() != '')]
    df = df.dropna(subset=['processo', 'dt_abertura', 'dt_faturamento',
                           'servico', 'vlr_faturamento'])

    # Calcular lead time e filtrar (0 a 365 dias)
    df['lead_time'] = (df['dt_faturamento'] - df['dt_abertura']).dt.days
    df = df[(df['lead_time'] >= 0) & (df['lead_time'] <= 365)]

    # Padronizar modal
    mapeamento_modal = {
        'AIRFREIGHT': 'Aéreo',
        'OCEANFREIGHT': 'Marítimo',
        'OCEANFREIGHT / FCL': 'Marítimo',
        'OCEANFREIGHT / LCL': 'Marítimo',
        'BREAK BULK': 'Marítimo',
        'RODOVIARIO': 'Rodoviário',
        'RODOVIÁRIO': 'Rodoviário'
    }
    df['modal'] = df['modal'].str.upper().str.strip().replace(mapeamento_modal)
    valores_desconhecidos = sorted(
        df.loc[~df['modal'].isin(['Aéreo', 'Marítimo', 'Rodoviário']), 'modal'].unique()
    )
    if valores_desconhecidos:
        print(f"Valores de modal descartados (fora do mapeamento): {valores_desconhecidos}")
    df = df[df['modal'].isin(['Aéreo', 'Marítimo', 'Rodoviário'])]

    # Remover outliers de valor (IQR por modal + serviço)
    print("\nRemovendo outliers de valor (IQR por modal + serviço)...")
    df_limpo = []

    for modal in ['Marítimo', 'Aéreo', 'Rodoviário']:
        for servico in df[df['modal'] == modal]['servico'].unique():
            df_grupo = df[(df['modal'] == modal) & (df['servico'] == servico)].copy()

            if len(df_grupo) < 10:
                df_limpo.append(df_grupo)
                continue

            antes = len(df_grupo)
            Q1, Q3 = df_grupo['vlr_faturamento'].quantile([0.25, 0.75])
            IQR = Q3 - Q1
            df_grupo = df_grupo[
                (df_grupo['vlr_faturamento'] >= Q1 - 1.5 * IQR) &
                (df_grupo['vlr_faturamento'] <= Q3 + 1.5 * IQR)
            ]
            depois = len(df_grupo)
            if antes - depois > 0:
                print(f"  {modal} - {servico}: removidos {antes - depois} outliers")

            df_limpo.append(df_grupo)

    df = pd.concat(df_limpo, ignore_index=True)
    print(f"Total após remoção de outliers: {len(df):,}")

    # Campos derivados
    df['mes_faturamento'] = df['dt_faturamento'].dt.month
    df['ano_faturamento'] = df['dt_faturamento'].dt.year
    df['mes_abertura']    = df['dt_abertura'].dt.month
    df['ano_abertura']    = df['dt_abertura'].dt.year

    print(f"\nProcessos finais: {len(df):,} ({len(df)/n_original*100:.1f}%)")
    print(f"Período: {df['dt_faturamento'].min().date()} a {df['dt_faturamento'].max().date()}")

    # Carregar metas
    df_metas = pd.read_csv(arquivo_metas, sep=';', encoding='utf-8-sig')
    df_metas['vlr_meta'] = (
        df_metas['vlr_meta']
        .astype(str)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
    )
    df_metas['vlr_meta'] = pd.to_numeric(df_metas['vlr_meta'], errors='coerce')
    df_metas['dt_meta'] = pd.to_datetime(df_metas['dt_meta'], format='%d/%m/%Y', errors='coerce')

    invalidos = df_metas[df_metas['vlr_meta'].isna() | df_metas['dt_meta'].isna()]
    if len(invalidos) > 0:
        print(f"AVISO: {len(invalidos)} linha(s) de metas descartadas por valor/data invalido:")
        print(invalidos.to_string(index=False))
        df_metas = df_metas.dropna(subset=['vlr_meta', 'dt_meta']).reset_index(drop=True)

    df_metas['mes'] = df_metas['dt_meta'].dt.month
    df_metas['ano'] = df_metas['dt_meta'].dt.year

    print(f"\nMetas carregadas: {len(df_metas)} registros")

    return df, df_metas
