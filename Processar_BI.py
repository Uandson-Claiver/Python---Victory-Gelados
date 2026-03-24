import pandas as pd
import os
import glob
import unicodedata
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH_BRUTOS = os.path.join(BASE_DIR, 'Dados Brutos')
PATH_MENSAL = os.path.join(PATH_BRUTOS, 'Faturamento_Mensal')
PATH_SAIDA  = os.path.join(BASE_DIR, 'Saída Limpa')
PATH_MESTRE = os.path.join(BASE_DIR, 'Tabela Mestre')

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

def iniciar_novo_projeto():
    # ---------------------------------------------------------
    # 1. PROCESSAMENTO DA DIMENSÃO CLIENTES (TABELA MESTRE)
    # ---------------------------------------------------------
    df_m = pd.read_csv(os.path.join(PATH_BRUTOS, 'Base_Clientes.csv'), sep=None, engine='python', encoding='latin1', dtype=str)
    df_m.columns = [c.strip() for c in df_m.columns]

    col_razao = next((c for c in df_m.columns if 'RAZ' in normalizar(c)), None)
    if not col_razao: 
        col_razao = next((c for c in df_m.columns if 'NOME' in normalizar(c) and 'FANTASIA' not in normalizar(c)), df_m.columns[1])

    df_m['Razao_Norm'] = df_m[col_razao].apply(normalizar)
    df_m['Fantasia_Norm'] = df_m['Nome Fantasia'].apply(normalizar)
    df_m['Cid_Norm'] = df_m['Cidade'].apply(normalizar)
    df_m['Doc_Limpo'] = df_m['CPF/CNPJ'].apply(limpar_doc)

    mapa_verdade = {}
    for _, r in df_m.iterrows():
        chave_busca = (r['Razao_Norm'], r['Fantasia_Norm'], r['Cid_Norm'])
        mapa_verdade[chave_busca] = r['Doc_Limpo']

    clientes_adicionais = []
    chaves_mestre_existentes = set(mapa_verdade.keys())

    # ---------------------------------------------------------
    # 2. PROCESSAMENTO DA FATO PATRIMÔNIOS
    # ---------------------------------------------------------
    df_p = pd.read_excel(os.path.join(PATH_BRUTOS, 'Base_Patrimonios.xlsx'), dtype=str)
    df_p.columns = [c.strip() for c in df_p.columns]
    
    cidade_col = [c for c in df_p.columns if 'CIDADE' in normalizar(c)][0]
    
    col_nr_pat = next((c for c in df_p.columns if 'NR' in normalizar(c) and 'PATRIMONIO' in normalizar(c)), 'Nr.Patrimônio')
    col_pat = next((c for c in df_p.columns if 'PATRIMONIO' in normalizar(c) and c != col_nr_pat), 'Patrimônio')
    col_vend = next((c for c in df_p.columns if 'VENDEDOR' in normalizar(c)), 'Vendedor')
    col_marca_p = next((c for c in df_p.columns if 'MARCA' in normalizar(c)), 'Marca')
    col_status = next((c for c in df_p.columns if 'STATUS' in normalizar(c)), 'Status')

    res_p = []
    for _, r in df_p.iterrows():
        razao_p = normalizar(r.get('Razão Social', ''))
        fantasia_p = normalizar(r.get('Cliente', ''))
        cid_p = normalizar(r[cidade_col])
        
        chave_busca = (razao_p, fantasia_p, cid_p)
        
        if chave_busca in mapa_verdade:
            doc_final = mapa_verdade[chave_busca]
        else:
            doc_bruto = str(r['CNPJ']) if 'CNPJ' in df_p.columns and pd.notna(r['CNPJ']) else str(r.get('CPF', ''))
            doc_final = limpar_doc(doc_bruto)
            if chave_busca not in chaves_mestre_existentes:
                clientes_adicionais.append({'Razao_Norm': razao_p, 'Fantasia_Norm': fantasia_p, 'Cid_Norm': cid_p, 'Doc_Limpo': doc_final, 'Representante': normalizar(r.get(col_vend, ''))})
                chaves_mestre_existentes.add(chave_busca)
                mapa_verdade[chave_busca] = doc_final
        
        chave_bi = f"{doc_final}_{razao_p}_{fantasia_p}_{cid_p}"
        res_p.append([chave_bi, doc_final, razao_p, fantasia_p, cid_p, normalizar(r.get(col_vend, '')), str(r.get(col_nr_pat, '')).replace('nan', ''), str(r.get(col_pat, '')).replace('nan', ''), str(r.get(col_marca_p, '')).replace('nan', ''), str(r.get(col_status, '')).replace('nan', '')])

    cols_p = ['CHAVE_BI', 'CPF/CNPJ', 'Razao_Social', 'Nome_Fantasia', 'Cidade', 'Representante', 'Nr_Patrimonio', 'Patrimonio', 'Marca', 'Status']
    pd.DataFrame(res_p, columns=cols_p).to_csv(os.path.join(PATH_SAIDA, 'Fato_Patrimonios.csv'), index=False, sep=';', encoding='utf-8-sig')

    # ---------------------------------------------------------
    # 3. PROCESSAMENTO DA FATO FATURAMENTO
    # ---------------------------------------------------------
    arquivos_f = glob.glob(os.path.join(PATH_MENSAL, "*.csv"))
    res_f_total = []
    for f in arquivos_f:
        df_f = pd.read_csv(f, sep=None, engine='python', encoding='latin1', dtype=str)
        df_f.columns = [c.strip() for c in df_f.columns]
        
        col_rep = next((c for c in df_f.columns if 'REPRESENTANTE' in normalizar(c)), 'Representante')
        col_mes = next((c for c in df_f.columns if 'MES' in normalizar(c)), None)
        col_op = next((c for c in df_f.columns if 'OPERA' in normalizar(c)), 'Operação')
        col_prod = next((c for c in df_f.columns if 'PRODUTO' in normalizar(c)), 'Produto')
        col_marca = next((c for c in df_f.columns if 'MARCA' in normalizar(c)), 'Marca')
        col_tot = next((c for c in df_f.columns if 'TOTAL' in normalizar(c)), 'Total Pedido')
        col_doc_f = next((c for c in df_f.columns if 'CPF' in normalizar(c) or 'CNPJ' in normalizar(c)), 'CPF/CNPJ Cliente')

        nome_base = os.path.splitext(os.path.basename(f))[0].upper()
        mes_extraido = nome_base.replace('FATURAMENTO_', '').replace('FATURAMENTO', '').replace('-', '').replace('_', '').strip()

        for _, r in df_f.iterrows():
            razao_f = normalizar(r.get('Cliente', ''))
            fantasia_f = normalizar(r.get('Nome Fantasia', ''))
            cid_f = normalizar(r.get('Cidade', ''))
            
            chave_busca = (razao_f, fantasia_f, cid_f)
            
            if chave_busca in mapa_verdade:
                doc_final = mapa_verdade[chave_busca]
            else:
                doc_final = limpar_doc(r.get(col_doc_f, ''))
                if chave_busca not in chaves_mestre_existentes:
                    clientes_adicionais.append({'Razao_Norm': razao_f, 'Fantasia_Norm': fantasia_f, 'Cid_Norm': cid_f, 'Doc_Limpo': doc_final, 'Representante': normalizar(r.get(col_rep, ''))})
                    chaves_mestre_existentes.add(chave_busca)
                    mapa_verdade[chave_busca] = doc_final
            
            val_mes = str(r.get(col_mes, '')).replace('nan', '').strip() if col_mes else ''
            if not val_mes: val_mes = mes_extraido

            chave_bi = f"{doc_final}_{razao_f}_{fantasia_f}_{cid_f}"
            res_f_total.append([chave_bi, doc_final, razao_f, fantasia_f, cid_f, normalizar(r.get(col_rep, '')), val_mes, str(r.get(col_op, '')).replace('nan', ''), str(r.get(col_prod, '')).replace('nan', ''), str(r.get(col_marca, '')).replace('nan', ''), str(r.get(col_tot, '')).replace('nan', '')])

    cols_f = ['CHAVE_BI', 'CPF/CNPJ', 'Razao_Social', 'Nome_Fantasia', 'Cidade', 'Representante', 'Mes_Faturamento', 'Operacao', 'Produto', 'Marca', 'Total_Pedido']
    df_fato_faturamento = pd.DataFrame(res_f_total, columns=cols_f)
    
    # ---------------------------------------------------------
    # --- A MÁGICA DA DATA (À PROVA DE BALAS) ---
    # ---------------------------------------------------------
    def arrumar_data(valor):
        v = str(valor).strip().upper()
        # 1. Se o sistema acabou puxando 'JAN', 'FEV' do nome do arquivo:
        meses = {'JAN': '01', 'FEV': '02', 'MAR': '03', 'ABR': '04', 'MAI': '05', 'JUN': '06', 
                 'JUL': '07', 'AGO': '08', 'SET': '09', 'OUT': '10', 'NOV': '11', 'DEZ': '12'}
        if v in meses:
            return f"2026-{meses[v]}-01" # Força o ano e o dia 01
            
        # 2. Se for qualquer outra coisa (como 2026/01 ou 01/2026), deixa o Pandas se virar
        return v

    # Aplica a nossa regra salva-vidas
    df_fato_faturamento['Mes_Faturamento'] = df_fato_faturamento['Mes_Faturamento'].apply(arrumar_data)
    
    # Pede pro Pandas descobrir o formato sozinho, sem forçar uma regra cega
    df_fato_faturamento['Mes_Faturamento'] = pd.to_datetime(df_fato_faturamento['Mes_Faturamento'], errors='coerce').dt.strftime('%Y-%m-%d')
    
    # Se ainda assim algo vier muito bizarro do ERP, preenche com uma data padrão para a linha não sumir do BI
    df_fato_faturamento['Mes_Faturamento'] = df_fato_faturamento['Mes_Faturamento'].fillna('2026-01-01')
    # ---------------------------------------------------------
    
    df_fato_faturamento.to_csv(os.path.join(PATH_SAIDA, 'Fato_Faturamento.csv'), index=False, sep=';', encoding='utf-8-sig')

    # ---------------------------------------------------------
    # 4. FINALIZAÇÃO DA DIMENSÃO CLIENTES (RESGATE DE FANTASMAS)
    # ---------------------------------------------------------
    if clientes_adicionais:
        df_novos = pd.DataFrame(clientes_adicionais)
        df_m = pd.concat([df_m, df_novos], ignore_index=True)

    df_m['CHAVE_BI'] = df_m['Doc_Limpo'] + "_" + df_m['Razao_Norm'] + "_" + df_m['Fantasia_Norm'] + "_" + df_m['Cid_Norm']
    
    cols_m_export = ['CHAVE_BI', 'Cód. Cliente', 'Razao_Norm', 'Fantasia_Norm', 'Doc_Limpo', 'Cid_Norm', 'Representante']
    for c in cols_m_export:
        if c not in df_m.columns:
            df_m[c] = "NAO_INFORMADO"
            
    df_m[cols_m_export].rename(columns={'Razao_Norm': 'Razao_Social', 'Fantasia_Norm': 'Nome_Fantasia', 'Doc_Limpo': 'CPF/CNPJ', 'Cid_Norm': 'Cidade'}).to_csv(os.path.join(PATH_MESTRE, 'Dim_Clientes.csv'), index=False, sep=';', encoding='utf-8-sig')

if __name__ == "__main__":
    iniciar_novo_projeto()
