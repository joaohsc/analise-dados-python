# Sobre o projeto
Neste projeto eu desenvolvi um script python para coletar dados de uma planilha xlsx e inserir em um banco de dados existente. 

**Tecnologias e ferramentas: python, postgresql, pandas, jupyter notebook**

# Instruções para rodar o projeto:
-  Criar uma virtualenv;
-  pip install -r requirements.txt;
-  Adicionar na pasta principal um arquivo .env para inserir os dados do banco dedados;
-  python main.py: comando para rodar o script;

# Variáveis do arquivo .env:
- DB_PASSWORD="insira a senha"
- DB_PORT="insira a porta"
- DB_HOST="localhost"
- DB_USERNAME="insira o usuário"
- DB_NAME="insira o nome do banco de dados"
