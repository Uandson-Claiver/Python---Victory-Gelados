import pandas as pd
import os
import glob
import unicodedata
import re

# --- CONFIGURAÇÃO DE CAMINHOS ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH_BRUTOS = os.path.join(BASE_DIR, 'Dados Brutos')
PATH_MENSAL = os.path.join(PATH_BRUTOS, 'Faturamento_Mensal')
PATH_SEM_COMPRA = os.path.join(PATH_BRUTOS, 'Sem_Compra_Mensal')
PATH_MESTRE = os.path.join(BASE_DIR, 'Tabela Mestre')
PATH_SAIDA  = os.path.join(BASE_DIR, 'Saída Limpa')
PATH_BACKUP = os.path.join(PATH_SAIDA, 'Clientes_Ociosos')

# --- FUNÇÕES DE UTILIDADE ---
def limpar_doc(doc):
    if pd.isna(doc) or str(doc).strip() == "": return "NAO_INFORMADO"
    d_str = str(doc).strip().upper()
    if d_str.endswith('.0'): d_str = d_str[:-2]
    if 'E+' in d_str:
        try: d_str = "{:.0f}".format(float(d_str))
        except: pass
    return re.sub(r'\D', '', d_str)

def normalizar(texto):
    if pd.isna(texto) or str(texto).strip() == "": return "NAO_INFORMADO"
    t = str(texto).strip().upper()
    t = unicodedata.normalize('NFKD', t).encode('ASCII', 'ignore').decode('ASCII')
    t = re.sub(r'[^A-Z0-9 ]', '', t)
    return " ".join(t.split())

def extrair_data_do_nome(nome_arquivo):
    meses_map = {
        'JAN': '01', 'FEV': '02', 'MAR': '03', 'ABR': '04', 'MAI': '05', 'JUN': '06',
        'JUL': '07', 'AGO': '08', 'SET': '09', 'OUT': '10', 'NOV': '11', 'DEZ': '12'
    }
    nome_up = nome_arquivo.upper()
    mes_num = "01"
    for sigla, num in meses_map.items():
        if sigla in nome_up:
            mes_num = num
            break
    ano_busca = re.search(r'20\d{2}', nome_up)
    ano = ano_busca.group(0) if ano_busca else "2026"
    return f"{ano}-{mes_num}-01"

# --- PROCESSO 1: FATURAMENTO E DIM_CLIENTES ---
def processar_faturamento_e_mestre():
    print("Processando Faturamento e Tabela Mestre...")
    df_m = pd.read_csv(os.path.join(PATH_BRUTOS, 'Base_Clientes.csv'), sep=None, engine='python', encoding='latin1', dtype=str)
    df_m.columns = [c.strip() for c in df_m.columns]
    col_razao = next((c for c in df_m.columns if 'RAZ' in normalizar(c)), df_m.columns[1])
    df_m['Razao_Norm'] = df_m[col_razao].apply(normalizar)
    df_m['Fantasia_Norm'] = df_m['Nome Fantasia'].apply(normalizar)
    df_m['Cid_Norm'] = df_m['Cidade'].apply(normalizar)
    df_m['Doc_Limpo'] = df_m['CPF/CNPJ'].apply(limpar_doc)

    mapa_verdade = { (r['Razao_Norm'], r['Fantasia_Norm'], r['Cid_Norm']): r['Doc_Limpo'] for _, r in df_m.iterrows() }
    clientes_adicionais = []
    chaves_mestre_existentes = set(mapa_verdade.keys())

    df_p_base = pd.read_excel(os.path.join(PATH_BRUTOS, 'Base_Patrimonios.xlsx'), dtype=str)
    df_p_base.columns = [c.strip() for c in df_p_base.columns]
    cidade_col = [c for c in df_p_base.columns if 'CIDADE' in normalizar(c)][0]
    col_nr_pat = next((c for c in df_p_base.columns if 'NR' in normalizar(c) and 'PATRIMONIO' in normalizar(c)), 'Nr.Patrimônio')
    col_vend = next((c for c in df_p_base.columns if 'VENDEDOR' in normalizar(c)), 'Vendedor')

    res_p = []
    for _, r in df_p_base.iterrows():
        raz_p, fan_p, cid_p = normalizar(r.get('Razão Social', '')), normalizar(r.get('Cliente', '')), normalizar(r[cidade_col])
        chave_busca = (raz_p, fan_p, cid_p)
        if chave_busca in mapa_verdade:
            doc_final = mapa_verdade[chave_busca]
        else:
            doc_bruto = str(r['CNPJ']) if 'CNPJ' in df_p_base.columns and pd.notna(r['CNPJ']) else str(r.get('CPF', ''))
            doc_final = limpar_doc(doc_bruto)
            if chave_busca not in chaves_mestre_existentes:
                clientes_adicionais.append({'Razao_Norm': raz_p, 'Fantasia_Norm': fan_p, 'Cid_Norm': cid_p, 'Doc_Limpo': doc_final, 'Representante': normalizar(r.get(col_vend, ''))})
                chaves_mestre_existentes.add(chave_busca)
                mapa_verdade[chave_busca] = doc_final
        res_p.append([f"{doc_final}_{raz_p}_{fan_p}_{cid_p}", doc_final, raz_p, fan_p, cid_p, normalizar(r.get(col_vend, '')), str(r.get(col_nr_pat, '')).replace('nan', ''), str(r.get('Patrimônio', '')).replace('nan', ''), str(r.get('Marca', '')).replace('nan', ''), str(r.get('Status', '')).replace('nan', '')])

    pd.DataFrame(res_p, columns=['CHAVE_BI', 'CPF/CNPJ', 'Razao_Social', 'Nome_Fantasia', 'Cidade', 'Representante', 'Nr_Patrimonio', 'Patrimonio', 'Marca', 'Status']).to_csv(os.path.join(PATH_SAIDA, 'Fato_Patrimonios.csv'), index=False, sep=';', encoding='utf-8-sig')

    arquivos_f = glob.glob(os.path.join(PATH_MENSAL, "*.csv"))
    res_f_total = []
    meses_map = {'JAN': '01', 'FEV': '02', 'MAR': '03', 'ABR': '04', 'MAI': '05', 'JUN': '06', 'JUL': '07', 'AGO': '08', 'SET': '09', 'OUT': '10', 'NOV': '11', 'DEZ': '12'}
    for f in arquivos_f:
        df_f = pd.read_csv(f, sep=None, engine='python', encoding='latin1', dtype=str)
        df_f.columns = [c.strip() for c in df_f.columns]
        nome_base = os.path.splitext(os.path.basename(f))[0].upper()
        ano_f = re.search(r'20\d{2}', nome_base).group(0) if re.search(r'20\d{2}', nome_base) else "2026"
        mes_f_padrao = "01"
        for sigla, num in meses_map.items():
            if sigla in nome_base:
                mes_f_padrao = num
                break
        col_rep = next((c for c in df_f.columns if 'REPRESENTANTE' in normalizar(c)), 'Representante')
        col_mes = next((c for c in df_f.columns if 'MES' in normalizar(c)), None)
        for _, r in df_f.iterrows():
            raz_f, fan_f, cid_f = normalizar(r.get('Cliente', '')), normalizar(r.get('Nome Fantasia', '')), normalizar(r.get('Cidade', ''))
            chave_busca = (raz_f, fan_f, cid_f)
            doc_final = mapa_verdade.get(chave_busca, limpar_doc(r.get('CPF/CNPJ Cliente', '')))
            val_col_mes = str(r.get(col_mes, '')).strip().upper() if col_mes else ""
            mes_final = meses_map.get(val_col_mes, mes_f_padrao)
            data_final = f"{ano_f}-{mes_final}-01"
            res_f_total.append([f"{doc_final}_{raz_f}_{fan_f}_{cid_f}", doc_final, raz_f, fan_f, cid_f, normalizar(r.get(col_rep, '')), data_final, str(r.get('Operação', 'nan')), str(r.get('Produto', 'nan')), str(r.get('Marca', 'nan')), str(r.get('Total Pedido', '0'))])

    df_fat = pd.DataFrame(res_f_total, columns=['CHAVE_BI', 'CPF/CNPJ', 'Razao_Social', 'Nome_Fantasia', 'Cidade', 'Representante', 'Mes_Faturamento', 'Operacao', 'Produto', 'Marca', 'Total_Pedido'])
    df_fat['Mes_Faturamento'] = pd.to_datetime(df_fat['Mes_Faturamento'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('2026-01-01')
    df_fat.to_csv(os.path.join(PATH_SAIDA, 'Fato_Faturamento.csv'), index=False, sep=';', encoding='utf-8-sig')

    if clientes_adicionais:
        df_m = pd.concat([df_m, pd.DataFrame(clientes_adicionais)], ignore_index=True)
    df_m['CHAVE_BI'] = df_m['Doc_Limpo'] + "_" + df_m['Razao_Norm'] + "_" + df_m['Fantasia_Norm'] + "_" + df_m['Cid_Norm']
    cols_m = ['CHAVE_BI', 'Cód. Cliente', 'Razao_Norm', 'Fantasia_Norm', 'Doc_Limpo', 'Cid_Norm', 'Representante']
    for c in cols_m:
        if c not in df_m.columns: df_m[c] = "NAO_INFORMADO"
    df_m[cols_m].rename(columns={'Razao_Norm': 'Razao_Social', 'Fantasia_Norm': 'Nome_Fantasia', 'Doc_Limpo': 'CPF/CNPJ', 'Cid_Norm': 'Cidade'}).to_csv(os.path.join(PATH_MESTRE, 'Dim_Clientes.csv'), index=False, sep=';', encoding='utf-8-sig')

# --- PROCESSO 2: RELATÓRIO DE OCIOSOS (HÍBRIDO: BI LONGO + EXCEL AGRUPADO) ---
def processar_ociosos():
    print("Processando Clientes Ociosos (BI = Detalhado | Excel = Agrupado)...")
    if not os.path.exists(PATH_BACKUP): os.makedirs(PATH_BACKUP)
    
    # 1. Base de Patrimônios
    df_p_raw = pd.read_excel(os.path.join(PATH_BRUTOS, 'Base_Patrimonios.xlsx'), dtype=str)
    col_nr_real = next((c for c in df_p_raw.columns if 'NR' in normalizar(c) and 'PATRI' in normalizar(c)), 'Nr.Patrimônio')
    df_p_raw['Razao_Social_Norm'] = df_p_raw['Razão Social'].apply(normalizar)
    df_pat_detalhado = df_p_raw[['Razao_Social_Norm', col_nr_real]].rename(columns={col_nr_real: 'Patrimonio_Individual'})

    # 2. Leitura dos Arquivos Sem Compra
    arquivos_sc = glob.glob(os.path.join(PATH_SEM_COMPRA, "*.csv"))
    if not arquivos_sc: return
    lista_dfs = []
    for f in arquivos_sc:
        data_ref = extrair_data_do_nome(os.path.basename(f))
        df_temp = pd.read_csv(f, sep=None, engine='python', encoding='latin1', dtype=str)
        # Limpeza de nomes de colunas
        df_temp.columns = [" ".join(re.sub(r'[^a-zA-Z0-9 ]', ' ', str(c)).split()).strip() for c in df_temp.columns]
        df_temp['Mes_Referencia'] = data_ref
        # Preenche vazios com "NAO INFORMADO" para não sumir no groupby
        df_temp = df_temp.fillna("NAO INFORMADO")
        df_temp['Cliente_Norm'] = df_temp['Cliente'].apply(normalizar)
        lista_dfs.append(df_temp)

    df_stack = pd.concat(lista_dfs, ignore_index=True)
    
    # 3. Cruzamento Detalhado (Power BI)
    df_final_bi = pd.merge(df_stack, df_pat_detalhado, left_on='Cliente_Norm', right_on='Razao_Social_Norm', how='left')
    df_final_bi['Patrimonio_Individual'] = df_final_bi['Patrimonio_Individual'].fillna('SEM COMODATO')
    df_final_bi['Qtd_Linha'] = df_final_bi['Patrimonio_Individual'].apply(lambda x: 0 if x == 'SEM COMODATO' else 1)
    
    df_csv_bi = df_final_bi.drop(columns=['Cliente_Norm', 'Razao_Social_Norm'], errors='ignore')
    df_csv_bi.to_csv(os.path.join(PATH_SAIDA, 'Relatorio_Ociosos_Final.csv'), index=False, sep=';', encoding='utf-8-sig')

    # --- EXCEL FIXO (AGRUPADO) ---
    
    # Identifica colunas para agrupar (evitando as de valores e a norm)
    cols_para_agrupar = [c for c in df_final_bi.columns if c not in ['Patrimonio_Individual', 'Razao_Social_Norm', 'Qtd_Linha', 'Cliente_Norm']]

    # CRITICAL FIX: dropna=False garante que linhas com algum campo vazio NÃO sumam
    df_excel_fixo = df_final_bi.groupby(cols_para_agrupar, as_index=False, dropna=False).agg({
        'Patrimonio_Individual': lambda x: ', '.join([str(i) for i in x.dropna() if str(i).strip() not in ["", "nan", "SEM COMODATO"]])
    })
    
    df_excel_fixo['Patrimonio_Individual'] = df_excel_fixo['Patrimonio_Individual'].replace('', 'SEM COMODATO')
    df_excel_fixo['Qtd_Equipamentos'] = df_excel_fixo['Patrimonio_Individual'].apply(lambda x: 0 if x == 'SEM COMODATO' else len(x.split(',')))
    
    # Formata data
    df_excel_fixo['Mes_Referencia'] = pd.to_datetime(df_excel_fixo['Mes_Referencia']).dt.date
    
    # Salva Excel
    data_snapshot = pd.Timestamp.now().strftime('%Y_%m_%d')
    nome_bkp_xlsx = os.path.join(PATH_BACKUP, f"Relatorio_Ociosos_FIXO_{data_snapshot}.xlsx")
    
    with pd.ExcelWriter(nome_bkp_xlsx, engine='xlsxwriter') as writer:
        df_excel_fixo.to_excel(writer, index=False, sheet_name='Ociosos_Agrupado')
        workbook, worksheet = writer.book, writer.sheets['Ociosos_Agrupado']
        formato_data = workbook.add_format({'num_format': 'dd/mm/yyyy'})
        
        for i, col in enumerate(df_excel_fixo.columns):
            max_val = df_excel_fixo[col].astype(str).str.len().max()
            largura = max(float(max_val or 0), len(str(col))) + 2
            if col == 'Mes_Referencia':
                worksheet.set_column(i, i, 22, formato_data)
            else:
                worksheet.set_column(i, i, largura)
        
        worksheet.autofilter(0, 0, len(df_excel_fixo), len(df_excel_fixo.columns) - 1)
    
    print(f"Relatório Concluído. Total de linhas no Excel: {len(df_excel_fixo)}")
    
    print(f"Processamento Concluído: BI Detalhado (.csv) | Backup Agrupado (.xlsx)")

# --- EXECUÇÃO ---
if __name__ == "__main__":
    print("=== INICIANDO PROCESSAMENTO MESTRE UNIFICADO ===")
    processar_faturamento_e_mestre()
    processar_ociosos()
    print("=== TUDO PRONTO! ===")
