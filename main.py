import numpy as np
import psycopg2
import pandas as pd
from db import conn, insert_values, select_values

def criar_tbl_contrato(clientes, planos, status_contratos, df):
    return 0

def criar_tbl_contato(clientes, tipo_contatos, df):
    clientes_df = pd.DataFrame(list(clientes))
    contatos_df = pd.DataFrame(list(tipo_contatos))
    print(contatos_df)

    
    cliente_contato_arr = []
    for row in clientes_df.values:
        cliente_infos = df.loc[df['CPF/CNPJ']==row[3]]

        celular = [ c for c in list(cliente_infos["Celulares"]) if c != None ] 
        telefone = [ t for t in list(cliente_infos["Telefones"]) if t != None ]
        email = [ e for e in list(cliente_infos["Emails"]) if e != None ]
        
        if celular:
            for cel in celular:
                cliente_contato_arr.append({
                    "cliente_id": row[0],
                    "tipo_contato_id" : 1, 
                    "contato" : cel        
                })
        if telefone:
            for tel in telefone:
                cliente_contato_arr.append({
                    "cliente_id": row[0],
                    "tipo_contato_id" : 2, 
                    "contato" : tel        
                })
        if email:
            for mail in email:
                cliente_contato_arr.append({
                    "cliente_id": row[0],
                    "tipo_contato_id" : 3, 
                    "contato" : mail        
                })
    
    cliente_contato_df = pd.DataFrame(cliente_contato_arr, 
                                      index=range(len(cliente_contato_arr)))
    return cliente_contato_df

def tratar_contrato(clientes, planos, status_contratos, df):
    contrato_df = criar_tbl_contato(clientes, planos, status_contratos, df)
    contrato_tuples = contrato_df.itertuples(index=False)
    return contrato_tuples

def tratar_cliente_contato(clientes, tipo_contatos, df):
    cliente_contato_df = criar_tbl_contato(clientes, tipo_contatos, df)
    cliente_contato_tuples = cliente_contato_df.itertuples(index=False)
    return cliente_contato_tuples

def tratar_planos(df):
    plano_df = df[['Plano', 'Plano Valor']].drop_duplicates().reset_index()
    plano_tratado_df = plano_df.drop(columns='index')
    plano_tuples = plano_tratado_df.itertuples(index=False)
    return plano_tuples
    
def tratar_clientes(df):
    cliente_df = df[['Nome/Razão Social', 'Nome Fantasia', 'CPF/CNPJ', 'Data Nasc.', 'Data Cadastro cliente']]
    cliente_tratado_df = cliente_df.drop_duplicates(subset=['CPF/CNPJ'], keep="last")
    cliente_tuples = cliente_tratado_df.itertuples(index=False)
    return cliente_tuples

def tratar_status_contrato(df):
    status_df = df['Status'].drop_duplicates().reset_index()
    status_df_tratado = status_df.drop(columns='index')
    return status_df_tratado

path = "dados/dados_importacao.xlsx"

df= pd.read_excel(path)
df= df.replace({np.nan: None})

#clientes_tratados = tratar_clientes(df)
#sql = """INSERT INTO tbl_clientes(nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro) 
#       VALUES(%s,%s,%s, %s, %s)"""

#insert_values(sql, clientes_tratados,context="clientes")


#planos_tratados = tratar_planos(df)
#sql = """INSERT INTO tbl_planos(descricao, valor) 
#        VALUES(%s,%s)"""

#insert_values(sql,planos_tratados, context="planos")

tbl_tipos_contato = select_values(
    table="tbl_tipos_contato",
    context="tipos_contato")

tbl_planos = select_values(
    table="tbl_planos",
    context="planos")

tbl_status_contrato = select_values(
    table="tbl_status_contrato",
    context="status_contrato")

tbl_clientes = select_values(
    table="tbl_clientes",
    context="clientes")

#cliente_contato_tratados = tratar_cliente_contato(tbl_clientes, tbl_tipos_contato, df)

#sql = """INSERT INTO tbl_cliente_contatos(cliente_id, tipo_contato_id, contato) 
#        VALUES(%s,%s,%s)"""

#insert_values(sql,cliente_contato_tratados, context="cliente_contatos")



