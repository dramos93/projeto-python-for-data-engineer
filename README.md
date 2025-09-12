# projeto-python-for-data-engineer
🚀 Projeto Final – Pipeline de Cotações Cambiais com Python + LLM


Problemas que tive:
Docker não iniciado, foi verificado usando:
systemctl status docker
Depois ligado:
sudo systemctl start docker
Para sempre ligar o docker quando o wsl ligar:
sudo systemctl enable docker

Dei um docker build -t pipeline-cotacoes . para construir a imagem..

rodei para testar: docker run pipeline-cotacoes:latest

executando o shell do container e usando o dado do repositório..

sudo docker run -it --rm -v $(pwd)/data:/src/data pipeline-ingestao
