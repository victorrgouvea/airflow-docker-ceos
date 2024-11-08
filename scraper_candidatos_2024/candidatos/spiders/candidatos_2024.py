import json
from scrapy import Spider, Request


class Candidatos2024Spider(Spider):
    name = "2024"

    def start_requests(self):
        # Todos os municipios de SC
        url = "https://divulgacandcontas.tse.jus.br/divulga/rest/v1/eleicao/buscar/SC/2045202024/municipios"
        yield Request(url, self.parse_municipios)
        
    def parse_municipios(self, response):
        data = json.loads(response.text)
        municipios = data["municipios"]
        
        # Para cada municipio, buscar os candidatos
        for municipio in municipios:
            
            # 11 - Prefeitos, 12 - Vice-Prefeitos, 13 - Vereadores
            url_prefeitos = f"https://divulgacandcontas.tse.jus.br/divulga/rest/v1/candidatura/listar/2024/{municipio['codigo']}/2045202024/11/candidatos"
            url_vice_prefeitos = f"https://divulgacandcontas.tse.jus.br/divulga/rest/v1/candidatura/listar/2024/{municipio['codigo']}/2045202024/12/candidatos"
            url_vereadores = f"https://divulgacandcontas.tse.jus.br/divulga/rest/v1/candidatura/listar/2024/{municipio['codigo']}/2045202024/13/candidatos"
            
            # Busca os 3 tipos de candidatos para cada municipio
            yield Request(url_prefeitos, self.parse_candidatos)
            yield Request(url_vice_prefeitos, self.parse_candidatos)
            yield Request(url_vereadores, self.parse_candidatos)
            
    def parse_candidatos(self, response):
        data = json.loads(response.text)
        cargo = data["cargo"]['codigo']
        codigo_cidade = data['unidadeEleitoral']['codigo']
        candidatos = data["candidatos"]
        
        # Pegando as informações de cada candidato
        for candidato in candidatos:
            yield Request(f"https://divulgacandcontas.tse.jus.br/divulga/rest/v1/candidatura/buscar/2024/{codigo_cidade}/2045202024/candidato/{candidato['id']}", 
                self.parse_candidato,
                meta={"cargo": cargo, "id_candidato": candidato["id"]}
            )

    def parse_candidato(self, response):
        data = json.loads(response.text)
        cargo = response.meta["cargo"]
        id_candidato = response.meta["id_candidato"]
        
        dados_candidato = {
            "id": data.get("id"),
            "nome": data.get("nomeCompleto"),
            "numUrna": data.get("numero"),
            "sexo": data.get("descricaoSexo"),
            "dataNasc": data.get("dataDeNascimento"),
            "tituloEleitor": data.get("tituloEleitor"),
            "cidadeNasc": data.get("nomeMunicipioNascimento"),
            "estadoNasc": data.get("sgUfNascimento"),
            "cidadeCandidatura": data.get("localCandidatura"),
            "estadoCandidatura": data.get("ufSuperiorCandidatura"),
            "fotoUrl": data.get("fotoUrl"),
            "situacaoAtual": data.get("descricaoTotalizacao"),
            "bens": data.get("bens"),
            "vice": data.get("vices", [])[0].get("nm_CANDIDATO") if data.get("vices") else None,
            "siglaPartido": data.get("partido").get("sigla"),
            "cargo": data.get("cargo").get("nome")
        }
        
        yield Request(f'https://divulgacandcontas.tse.jus.br/divulga/rest/v1/prestador/consulta/2045202024/2024/{data["ufCandidatura"]}/{cargo}/{data["partido"]["numero"]}/{data["numero"]}/{id_candidato}',
            self.parse_financeiro,
            meta={"dados_candidato": dados_candidato}
        )
        
    def parse_financeiro(self, response):
        data = json.loads(response.text)
        dados_candidato = response.meta["dados_candidato"]
         
        dados_financeiros = {
            "totalLiquidoCampanha": data.get("dadosConsolidados").get("totalRecebido") if data.get("dadosConsolidados") else None,
        }
        
        if data.get('idPerstador') and data.get('idUltimaEntrega'):
            yield Request(
                f"https://divulgacandcontas.tse.jus.br/divulga/rest/v1/prestador/consulta/receitas/2045202024/{data['idPrestador']}/{data['idUltimaEntrega']}/rank",
                self.parse_doadores,
                meta={
                    "dados_candidato": {**dados_candidato, **dados_financeiros},
                    "idPrestador": data['idPrestador'],
                    "idUltimaEntrega": data['idUltimaEntrega']
                }
            )
        else:
            yield {**dados_candidato, **dados_financeiros}
        
    def parse_doadores(self, response):
        data = json.loads(response.text)
        dados_candidato = response.meta["dados_candidato"]
        doadores = {"doadores": data}
        
        yield Request(
            f"https://divulgacandcontas.tse.jus.br/divulga/rest/v1/prestador/consulta/despesas/2045202024/{response.meta['idPrestador']}/{response.meta['idUltimaEntrega']}",
            self.parse_despesas,
            meta={
                "dados_candidato": {**dados_candidato, **doadores},
                "idPrestador": response.meta['idPrestador'],
                "idUltimaEntrega": response.meta['idUltimaEntrega']
            }
        )
        
    def parse_despesas(self, response):
        data = json.loads(response.text)
        dados_candidato = response.meta["dados_candidato"]
        despesas = {"despesas": data}
        
        yield {**dados_candidato, **despesas}