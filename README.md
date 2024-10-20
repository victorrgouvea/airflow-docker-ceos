# airflow-docker-ceos
Airflow templates repository to be used in CEOS project


### Setup Inicial

1 - instalar docker e docker compose (se usar linux. Se for Mac ou Windows, o compose já vem instalado no docker) - download [aqui](https://docs.docker.com/get-started/get-docker/|aqui)

2 - instalar [git](https://git-scm.com/downloads) e sugiro fazer setup de SSH. [Como fazer isso](https://www.theserverside.com/blog/Coffee-Talk-Java-News-Stories-and-Opinions/GitHub-SSH-Key-Setup-Config-Ubuntu-Linux)

3 - clonar repositorio https://github.com/FabiSchwarz/airflow-docker-ceos (caso tenha feito o setup do ssh, utilize o comando `git clone git@github.com:FabiSchwarz/airflow-docker-ceos.git`)

4 - abra o docker

5 - dentro da pasta raiz clonada, abra um terminal e digite `docker-compose up airflow-init`. Isto instalará o airflow no docker

6 - finalizada a instalação, digitar `docker-compose up`  no terminal - isso vai levantar o airflow no localhost, assim como postgres, iceberg e spark, em seus devidos lugares

7 - em um navegador, digitar `localhost:8080` - usuário e senha são `airflow`





### Para rodar airflow após o setup inicial

- sempre abra o docker antes de mais nada

- dentro da pasta raiz, rodar `docker-compose up`

- em um navegador, digitar `localhost:8080` - usuário e senha são `airflow`

___________________________________________________________________________________

Para *DESLIGAR* o docker/airflow, digitar na linha de comando `docker-compose down`