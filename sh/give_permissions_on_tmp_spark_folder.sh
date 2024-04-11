#!/bin/bash

# Caso o seu sistema operacional seja Linux, é necessário executar esse script para dar permissão ao usuário glue_user para acessar a pasta /tmp/spark-events.
# Toda vez que o ambiente de desenvolvimento é "buildado", esse script deve ser executado novamente.
# Em Windows não é necessário executar esse script.
# Altere a variável CONTAINER_NAME para o nome do container que está rodando o ambiente de desenvolvimento.

CONTAINER_NAME=AWS_GLUE_SPARK_JOIN_EXAMPLES_DEV_CONTAINER
docker exec -u 0 $CONTAINER_NAME bash -c 'chown -R glue_user /tmp/spark-events'