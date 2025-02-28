# 📌 Automação de ETL e Controle de Carga

## 📜 Descrição do Projeto
Este repositório contém um conjunto de scripts Qlik que automatizam o processo de ETL (Extração, Transformação e Carga) de arquivos SQL e QVD. O objetivo é garantir a execução eficiente e monitorada de cargas de dados, organizando o processamento de arquivos SQL, a extração de informações e a geração de dados otimizados para uso em análises e relatórios.

## 📂 Estrutura do Código
O código está estruturado em diversas seções para facilitar a organização e a execução do fluxo de trabalho:

### 📁 Conexões
Nesta seção, são definidas as conexões com os diretórios e servidores utilizados no processo:
```qlik
LET SQLGitLab = 'SQLGitLab';
LET ETL = 'ETL';
LET ETLDesktop = 'ETLDesktop';
LET HistoricoControleCarga = 'HistoricoControleCarga';
LET vServer = ComputerName();
```
Essas variáveis representam os diretórios onde os arquivos serão processados e armazenados. A variável `vServer` captura o nome do servidor onde a execução está ocorrendo.

### 📋 Listagem de Arquivos SQL e QVD
O script percorre recursivamente os diretórios especificados, identificando arquivos SQL e QVD para processamento. Ele executa as seguintes ações:

1. **Busca de Arquivos SQL**:
   - O script percorre os diretórios configurados e identifica arquivos `.sql`.
   - Para cada arquivo encontrado, armazena as seguintes informações:
     - Pasta raiz e banco de dados associado.
     - Identificação do tipo de banco de dados (Oracle, MongoDB, SQL Server, MySQL, PostgreSQL).
     - Definição do servidor responsável pelo processamento.
     - Caminho completo do arquivo SQL.

2. **Busca de Arquivos QVD**:
   - Similar à busca de arquivos SQL, mas focado em arquivos `.qvd`.
   - Captura o nome do arquivo e seu tamanho.

```qlik
SUB RecursiveFileSearch(vPastaAtual)
    FOR EACH vFile IN FileList('$(vPastaAtual)/*.sql')
        ListaSQL:
        LOAD
            RowNo() AS ID_SQL,
            SubField('$(vFile)', '/', 3) AS Pasta_Raiz_SQL, 
            SubField('$(vFile)', '/', 4) AS Pasta_DataBase_SQL,              
            ...
        AUTOGENERATE 1;
    NEXT
    ...
END SUB
```
Após a busca, a lista de arquivos é armazenada para uso posterior no processo de ETL.

### 🔄 Controle de Processamento
O controle de processamento é responsável por garantir a execução ordenada dos arquivos. Ele funciona da seguinte maneira:
- Verifica se existe um histórico de execução salvo no `ControleProcessamento.qvd`.
- Se houver registros, carrega os arquivos e verifica se ainda há pendências.
- Se todos os arquivos anteriores foram processados, inicia um novo ciclo de carga.
- Se houver arquivos pendentes, prioriza aqueles que ainda não foram concluídos.
- Registra informações de execução, como status do arquivo, prioridade, horário de início e fim da carga.

Caso um arquivo não tenha sido concluído, ele é priorizado na próxima execução.

### 🚀 Execução do ETL
Para cada arquivo identificado na lista de controle:
1. O conteúdo SQL é lido e executado na base de dados correspondente.
2. Os resultados são armazenados em arquivos `.qvd` dentro das pastas ETL e ETLDesktop.
3. O status do processamento é atualizado na tabela de controle.

Se houver falhas na execução, o script tenta novamente conforme um mecanismo de **tentativa e erro** com um número máximo de tentativas e tempo de espera entre elas.

```qlik
SET MaxRetries = 10;
SET RetryCount = 0;
SET RetryDelay = 30 * 60;
SET RetrySuccess = 0;
SET ErrorMode = 0;

DO
    TRACE Tentativa $(RetryCount) de $(MaxRetries)...;
    STORE ControleProcessamento INTO [lib://$(SQLGitLab)/ControleProcessamento.qvd] (qvd);
    IF ScriptError = 0 THEN
        LET RetrySuccess = 1;
    ELSE
        LET RetryCount = $(RetryCount) + 1;
        TRACE Erro ao gravar o arquivo. Tentativa $(RetryCount) falhou. Tentando novamente...;
        Sleep $(RetryDelay) * 1000;
    ENDIF
LOOP WHILE $(RetrySuccess) = 0 AND $(RetryCount) < $(MaxRetries)
```

### ✅ Finalização e Validação
Ao final da execução:
- O script valida se todos os arquivos foram processados corretamente.
- Se houver arquivos pendentes, ele reexecuta o processo até um número máximo de tentativas.
- Se todos os arquivos foram processados, os registros são atualizados como **"Concluído"**.
- Caso alguma tentativa falhe repetidamente, o processo é encerrado com uma mensagem de erro para análise manual.

### 🔄 Reexecução de Arquivos Pendentes
Se houver falhas ou arquivos não processados corretamente, o sistema:
- Verifica se há arquivos com status **"Em_Andamento"**.
- Se encontrado, tenta processá-los novamente.
- Atualiza a tabela de controle para refletir as novas tentativas.
- Se um arquivo continuar com erro após múltiplas tentativas, o script gera uma mensagem para verificação manual.

## 📌 Tecnologias Utilizadas
- **Qlik Sense / QlikView** – Para automação de ETL e geração de QVDs
- **SQL** – Para execução de consultas e extração de dados
- **QVD (QlikView Data)** – Para armazenamento e otimização de dados processados

## 📌 Como Utilizar
1. **Configurar Conexões:**
   - Ajustar os diretórios das conexões no início do script.
   - Verificar se os caminhos configurados correspondem às pastas reais.
2. **Executar o Script:**
   - O processo será iniciado automaticamente, identificando e processando os arquivos pendentes.
   - Durante a execução, os arquivos são processados e armazenados em formato `.qvd`.
3. **Monitoramento:**
   - Acompanhar o arquivo `ControleProcessamento.qvd` para verificar o status de execução.
   - Caso existam falhas, verificar logs para análise do erro.
4. **Reexecução Manual:**
   - Caso necessário, apagar os registros do `ControleProcessamento.qvd` e executar novamente.

## 📌 Autor
Este projeto foi desenvolvido para automação e otimização de cargas de dados em ambientes Qlik, garantindo maior eficiência e rastreabilidade no processo de ETL.

Se precisar de mais detalhes ou ajustes, sinta-se à vontade para contribuir!

