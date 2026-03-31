import pandas as pd
import os
import glob
import unicodedata
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH_BRUTOS = os.path.join(BASE_DIR, 'Dados Brutos')
PATH_MENSAL = os.path.join(PATH_BRUTOS, 'Faturamento_Mensal')
PATH_SEM_COMPRA = os.path.join(PATH_BRUTOS, 'Sem_Compra_Mensal')
PATH_SAIDA  = os.path.join(BASE_DIR, 'Saída Limpa')
PATH_MESTRE = os.path.join(BASE_DIR, 'Tabela Mestre')
PATH_BACKUP = os.path.join(PATH_BRUTOS, 'Clientes_Ociosos')

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

def processar_faturamento_e_mestre():
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

def processar_ociosos():
    print("Iniciando Processamento de Clientes Ociosos...")
    if not os.path.exists(PATH_BACKUP): os.makedirs(PATH_BACKUP)
    
    # Lendo a base de patrimônios
    df_p_ocioso = pd.read_excel(os.path.join(PATH_BRUTOS, 'Base_Patrimonios.xlsx'), dtype=str)
    col_nr_real = next((c for c in df_p_ocioso.columns if 'NR' in normalizar(c) and 'PATRI' in normalizar(c)), None)
    if not col_nr_real: 
        print("ERRO: Coluna de patrimônio não encontrada.")
        return

    df_p_ocioso['Razao_Social_Norm'] = df_p_ocioso['Razão Social'].apply(normalizar)
    df_pat_limpo = df_p_ocioso[['Razao_Social_Norm', col_nr_real]].copy()
    df_pat_limpo.rename(columns={col_nr_real: 'Patrimonio_Individual'}, inplace=True)

    # Lendo arquivos de Sem Compra
    arquivos_sc = glob.glob(os.path.join(PATH_SEM_COMPRA, "*.csv"))
    if not arquivos_sc: 
        print("Aviso: Nenhum arquivo de Sem Compra encontrado.")
        return

    lista_dfs = []
    for caminho_completo in arquivos_sc:
        nome_arquivo = os.path.basename(caminho_completo)
        data_referencia = extrair_data_do_nome(nome_arquivo)
        df_temp = pd.read_csv(caminho_completo, sep=None, engine='python', encoding='latin1', dtype=str)
        df_temp.columns = [" ".join(re.sub(r'[^a-zA-Z0-9 ]', ' ', str(c)).split()).strip() for c in df_temp.columns]
        df_temp['Mes_Referencia'] = data_referencia
        df_temp['Cliente_Norm'] = df_temp['Cliente'].apply(normalizar)
        lista_dfs.append(df_temp)

    df_stack = pd.concat(lista_dfs, ignore_index=True)
    df_final_ocioso = pd.merge(df_stack, df_pat_limpo, left_on='Cliente_Norm', right_on='Razao_Social_Norm', how='left')
    df_final_ocioso['Patrimonio_Individual'] = df_final_ocioso['Patrimonio_Individual'].fillna('SEM COMODATO')
    df_final_ocioso['Qtd_Linha'] = df_final_ocioso['Patrimonio_Individual'].apply(lambda x: 0 if x == 'SEM COMODATO' else 1)
    df_final_ocioso['Mes_Referencia'] = pd.to_datetime(df_final_ocioso['Mes_Referencia'], errors='coerce')
    df_final_ocioso = df_final_ocioso.drop(columns=['Cliente_Norm', 'Razao_Social_Norm'], errors='ignore')
    
    # Salva o CSV para o Power BI
    df_final_ocioso.to_csv(os.path.join(PATH_SAIDA, 'Relatorio_Ociosos_Final.csv'), index=False, sep=';', encoding='utf-8-sig')
    
    # Salva o arquivo .xlsx dos ociosos e joga na pasta Clientes_Ociosos devidamente formatado
    data_snapshot = pd.Timestamp.now().strftime('%Y_%m_%d')
    nome_bkp_xlsx = os.path.join(PATH_BACKUP, f"Relatorio_Ociosos_FIXO_{data_snapshot}.xlsx")
    
    with pd.ExcelWriter(nome_bkp_xlsx, engine='xlsxwriter') as writer:
        df_final_ocioso.to_excel(writer, index=False, sheet_name='Ociosos')
        workbook = writer.book
        worksheet = writer.sheets['Ociosos']
        
        #cria formato de data abreviada
        formato_data = workbook.add_format({'num_format': 'dd/mm/yyyy'})
        
        for i, col in enumerate(df_final_ocioso.columns):
            # medição de largura de colunas
            max_val = df_final_ocioso[col].astype(str).str.len().max()
            if pd.isna(max_val): max_val = 0
            largura = max(max_val, len(str(col))) + 3
            
            #aplicando formatação de largura apenas na coluna de data
            if col == 'Mes_Referencia':
                worksheet.set_column(i, i, largura, formato_data)
            else:
                worksheet.set_column(i, i, largura)
        
        worksheet.autofilter(0, 0, len(df_final_ocioso), len(df_final_ocioso.columns) - 1)
    print(f"Relatório Excel formatado e salvo em: {nome_bkp_xlsx}")

#execução do script
if __name__ == "__main__":
    processar_faturamento_e_mestre()
    processar_ociosos()
