# CENIPA ETL Pipeline
## Overview

Com o objetivo de aprimorar minhas habilidades em construção de ETL de dados e explorar novas ferramentas e tecnologias de desenvolvimento na área de dados, embarquei em um projeto que utiliza os dados do CENIPA (Centro de Investigação e Prevenção de Acidentes Aeronáuticos). 
Esses dados contêm informações sobre: 

* Ocorrências Aeronáuticas.
* Tipos de ocorrências aeronáuticas
* Aeronaves envolvidas nas ocorrências aeronáuticas
* Fatores contribuintes das ocorrências aeronáuticas
* Recomendações de Segurança emitidas nas investigações de acidentes

Com base nesses dados, todo o projeto foi desenvolvido utilizando a plataforma cloud AWS como infraestrutura principal. Ao longo do processo, foram aproveitadas as seguintes ferramentas da AWS:

* **Lambda Function** - Para processamento serveless.
* **Step Function** - Para orquestração.
* **RDS (MySQL)** - Para processamento analítico
* **Glue Connection** - Para armazenar credencias de acessos ao DB.
* **S3** - Para armazenamento dos dados
* **DynamoDb** - Para armazenamento de logs.
* **ECR** - Para versionamento das lambdas.

No desenvolvimento da Lambda Function, optei pela a linguagem de programação Python onde utilizei as seguintes bibliotecas para a implementação da função:

* **Polars** - Para manipulação de dados.
* **Aws Wrangler** - Para interação com serviços AWS.
* **Connectorx** - Para leitura de tabelas em banco de dados. 

A escolha de utilizacao das bibliotecas foram feitas com base na exploração de novos tecnologias, buscando aprimorar diversos aspectos do projeto tais como: 

1. Performance ao ler grandes arquivos de dados.
2. Melhor interação com os servicos da AWS.
3. Maior velocidade em leitura de dados diretamenta de tabelas vindo do banco de dados.

Para a parte de infraestrutura deste projeto, foi utilizado a ferramenta **Terraform** para contrução e gerenciamento dos serviços AWS. Com o terraform é possivel implementar uma infraestrutura como codigo, facilitando definir toda a infraestrutura necessaria de forma declarativa. <br>
Desta forma utlizando o terraform é possivel proporcionar uma automatizacao e flexibilidade na utilização de servições AWS.

O projeto faz a utilização do **Github Actions** para realizar o deploy de forma automatizada dos serviçõs *Lambda Function* e *Step Function*.