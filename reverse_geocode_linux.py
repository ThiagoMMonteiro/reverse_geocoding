#!/usr/bin/env python
# coding: utf-8

# # Desafio 4all
#     -Realiza o geocode reverso de coordenadas geográficas e coloca os respectivos endereços em um banco de dados.
# ###### Autor: Thiago Mânica Monteiro
# ###### Contato: thiago.monteiro2608@gmail.com
# ###### Date: 23/03/2020
# ###### Descrição geral: 
#     
#         Esta rotina faz a extração de dados de arquivos texto, os quais possuem coordenadas geográficas e mais algumas informações que podem ser relevantes dependendo do contexto. 
#         Neste contexto, o relevante é apenas a geometria  referente a latitude e longitude. Assim, a rotina trata esses dados para obtermos apenas estes valores. 
#         Com cada latitude e longitude em uma estrutura Python, utilizaremos a geocodificação reversa para buscar no provedor o endereço a que se refere. 
#		  Esses endereços serão tratados e colocados em um banco de dados SQLite. A busca do endereço através da API utilizada e a inserção do endereço no banco de dados 
#		  é feita com a   utilização de threads para dar mais performance à solução, nos casos em que a API versão free permite.
# 

# ### Escolha de API 
# 
# **IMPORTANTE:** GoogleV3, em sua versão não paga, possui **limites** estabelecidos para a quantidade de requisições e a forma (tempo) como elas são realizadas. 
#				  O GoogleV3 permite até 50 requisições por segundo (múltiplas threads), porém, na versão free, você ganha apenas 40 mil requisições por mês (200 dólares). 
#                 Se a conta for nova, ganhará mais 20 mil no primeiro mês.  Estas limitações estão explicadas em detalhes no arquivo de documentação documentacao_desafio.doc.
#        
#    **GoogleV3:** 28.415 requisições ainda possíveis (lembrando que os 3 .txts possuem em torno de 3k pares de coordenadas) e com múltiplas threads (respeitando o limite de 50 requisições por segundo)
#   
#    Não seguir os termos de serviço da API pode resultar na exceção "Too Many requests", impossibilitando a chamada do método reverse.
# 

# ### Módulos utilizados
# 
# **geopy** - Possibilita o geocode reverso;
# **sqlite3** - Banco de dados SQLite;
# **threading** - Possibilita a utilização de threads;
# **queue** - Utilizado em conjunto com as threads, possui internamente mecanismos para "lockar" estruturas durante a execução das threads, utilizando filas;
# **time** - Utilização de sleeps nas threads, caso necessário;
# **GoogleV3** - Provedor 1 (API);

import geopy  
import sqlite3 
import threading 
import queue 
import time
import geopy.geocoders
from geopy.geocoders import GoogleV3


# ### Conexão API - Definição do geolocator
# 
#    O parâmetro **api_key** deve ser utilizado para acesso a API GoogleV3. É através dele que é feito o controle de quantas 
# requisições foram feitas em um determinado tempo, por exemplo. A chave é "individual" e possui um limite de utilização 
# gratuito, pertencente a um CPF ou CNPJ. Através do **reverse** do geopy, faremos nossas requisições ao provedor. 
# A chave de API do GoogleV3 pode ser modificada na célula abaixo, ou pode ser utilizada esta mesma chave, até alcançar seu limite.

#Definição do geolocator
geolocator = GoogleV3(api_key = 'AIzaSyDsZMAP68b8AyWvsBapGZSSDYuF-uZ5eqY', timeout = None)
reverse = geolocator.reverse

# ### Leitura dos arquivos texto
def read_file(archive_name):
    """Extrai pares de geometria latitude e longitude de um arquivo texto.

    Esta função separa cada linha de um arquivo texto individualmente pelos espaços em branco. Procura pela latitude e 
    insere em uma variável lat, procura pela sua longitude referente e insere em uma varável lon. Posteriormente é inserido
    em uma lista, as tuplas (lat, lon).

    Args:
        archive_name (str): O nome do arquivo texto Ex: data_points_20180101.txt

    Returns:
        Uma lista de tuplas contendo um par de coordenadas.
    example:
        [('-30.04982864', '-51.20150245'),
         ('-30.06761588', '-51.23976111'),
         ('-30.05596474', '-51.17286827')]
    
    Note:
        Esta função trata uma anomalia recorrente nos arquivos textos em que a latitude ou a longitude vem sozinha. 
        Para isso, ao fazer a inserção - lat_lon.append((lat,lon)) - somente quando acha a longitude, resolvemos o problema
        de quando não temos a longitude e, através de uma flag, resolvemos o problema de quando não vem a latitude
    """
    lat_lon = [] 
    with open(archive_name) as arquivo:
        for linha in arquivo:
            geometry = linha.split(' ')
            if geometry[0].find('Latitude:') != -1:
                lat = geometry[4].strip()
                flag = True
            elif geometry[0].find('Longitude:') != -1 and flag == True:
                lon = geometry[4].strip()
                lat_lon.append((lat,lon))
                flag = False
        arquivo.close()
    return lat_lon


# ### Tratamento do endereço retornado
def getAddr(location): 
    """Trata o endereço (objeto do tipo Location) retornado pela API para inserção no banco de dados.

    Esta função trata o objeto Location para retirar somente os dados que serão inseridos no banco de dados.

    Args:
        location (Objeto Location): Objeto do tipo "geopy.location.Location" retornado pela função reverse, que contém, 
        dentre outras informações, o endereço requisitado relativo à coordenada geográfica.

    Returns:
        Uma lista "addr" contendo os detalhes do endereço, que será futuramente armazenado em um banco de dados.
    example:
        [-30.05020045,
         -51.20177208,
         'Rua Monsenhor Veras',
         '361',
         'Santana',
         'Porto Alegre',
         '90610-010',
         'Rio Grande do Sul',
         'Brasil']
     
    Note:
        Na primeira linha da função, setamos as varáveis com a string default "no content returned" apenas para fins de 
        visualização, isso porque nem sempre o endereço relativo à coordenada retorna um número da casa ou um CEP, 
        por exemplo. Além de existirem coordenadas localizadas em um Rio, lago, etc, nem todos os retornos possuem algumas 
        informações que precisamos, dependendo da API, sua precisão e base de dados. 
    """
    road = house_number = suburb = city = postcode = state = country = 'no content returned'

    for i in range(len(location.raw['address_components'])): 
        if 'route' in location.raw['address_components'][i]['types']:
            road = location.raw['address_components'][i]['short_name']

        if 'street_number' in location.raw['address_components'][i]['types']:
            house_number = location.raw['address_components'][i]['short_name']

        if 'sublocality' in location.raw['address_components'][i]['types']:
            suburb = location.raw['address_components'][i]['short_name']

        if 'administrative_area_level_2' in location.raw['address_components'][i]['types']:
            city = location.raw['address_components'][i]['short_name']

        if 'postal_code' in location.raw['address_components'][i]['types']:
            postcode = location.raw['address_components'][i]['short_name']

        if 'administrative_area_level_1' in location.raw['address_components'][i]['types']:
            state = location.raw['address_components'][i]['short_name']

        if 'country' in location.raw['address_components'][i]['types']:
            country = location.raw['address_components'][i]['long_name']

    addr = [location.latitude, location.longitude, road, house_number, suburb, city, postcode, state, country]
    return addr


# ### Banco de dados
# O banco de dados utilizado foi o **SQLite**, pois de forma muito simples consegue satisfazer a nossa necessidade neste desafio. 
# Os comandos ddl *create* e *insert*, além da conexão e criação do banco foram deixados neste arquivo e não modularizados para termos um entendimento sequencial de como foi 
# durante o desenvolvimento.

def create_table(c):
    """Cria a tabela no banco de dados.

    Caso a tabela ainda não tenha sido criada, este procedimento criará. Apenas as colunas latitude e longitude são 
    do tipo float, as demais colunas são do tipo string.

    Args:
        c (objeto cursor()): O cursor criado na função main para que possamos realizar os comandos ddl.
    """
    c.execute('CREATE TABLE IF NOT EXISTS addresses (latitude float, longitude float, rua string, numero string,                 bairro string, cidade string, cep string, estado string, pais string)')


def dataentry(addr, c, connection):
    """Insere os dados do endereço no banco.

    Aqui é ralizado o comando INSERT para a inserção dos dados do endereço no banco de dados, na tabela addresses.

    Args:
        addr (list): O endereço já formatado, retornado pela função getAddr()
        c (objeto cursor()): O cursor criado na função main para que possamos realizar os comandos ddl.
        connection (connect()): a conexão criada com o banco na função main()
    """
    c.execute('INSERT INTO addresses (latitude, longitude, rua, numero, bairro, cidade, cep, estado, pais)                 VALUES (?,?,?,?,?,?,?,?,?)', addr)
    connection.commit()


# ### Threads
#    A utilização de threads foi utilizada para dar mais performance a solução. Neste contexo, foram criados duas funções: **callProducers(args)** e **callConsumers(args)**, 
#	 além de duas classes, sendo as threads propriamente ditas: **producerThread** e **consumerThread**.
#    O modelo utilizado foi o Produtor-Consumidor. As threads produtoras realizam a busca do endereço através da função reverse do geopy e armazenam o endereço formatado em 
#    uma **fila**. As threads consumidoras retiram os endereços desta fila e inserem no banco de dados SQLite.
#    
#    **Importante**: O maior gargalo no tempo de execução é a busca do endereço na internet, a requisição propriamente dita.
#    Qualquer processo de escrita é naturalmente mais lento que um mesmo processo de leitura. Porém, mesmo criando muitas threads consumidoras (escrita no banco), 
#	 não teríamos um aumento significativo de velocidade considerando a versão free das APIs testadas. Isto é: mesmo criando apenas uma thread consumidora e 
#	 diversas threads produtoras sem estourar o limite imposto pela API para as requisições, a nossa thread consumidora dá conta de escrever no banco num tempo razoável, 
#	 ficando bastante tempo ociosa. Por este motivo, a quantidade de threads consumidoras será sempre **uma**, enquanto as threads produtoras podem ser em qualquer quantidade, 
# 	 parametrizável na função main, a depender das limitações das APIs anteriormente mencionadas.

def callProducers(amount_producers, lat_lon): 
    """Cria as threads produtoras.

    Primeiramente, é feita a divisão da quantidade de requisições de cada thread produtora.
    Exemplo: se teremos 5 threads produtoras e 1000 pares de coordenadas, cada thread ficará com duzentas requisições.
    1000(tamanho de lat_lon)/5(quantidade de threads produtoras) = 200 requisições cada thread. Caso esta divisão não
    seja inteira, a última thread a ser criada ficará responsável por algumas poucas requisições a mais("queries_surp").
    "init_index" é o índice inicial da lista lat_lon onde cada thread começará a execução. 
    "my_range" representa a quantidade de requisições que cada thread irá fazer, representando o índice final
    das requisições para cada thread.

    Args:
        amount_producers (int): Quantidade de threads produtoras definidas na função main de forma parametrizada 
        (Podendo ser qualquer número não ferindo os termos de serviço da API).
        lat_lon (list): Lista contendo as tuplas de latitude e longitude.
    
    Returns:
        producers (list): Lista de threads produtoras
    """
    producers = []
    
    queries = int(len(lat_lon)/amount_producers)
    queries_surp = len(lat_lon) - (queries*amount_producers)

    init_index = 0
    my_range = queries 
    
    for p in range(amount_producers):
        if p == amount_producers-1:
            my_range = my_range+queries_surp
        producer = producerThread(p, init_index, my_range, lat_lon)
        producer.start()
        producers.append(producer)
        print('Thread ID: ', p)
        print('init_index: ', init_index)
        print('my_range: ', my_range)
        print('=========================')
        my_range += queries
        init_index += queries  
    return producers

def callConsumers(amount_consumers, c, connection): 
    """Cria as threads consumidoras.

    Esta função cria a thread consumidora. Para este desafio, apenas uma thread consumidora é criada, pois já é sufi
    ciente para realizar a tarefa em um tempo razoável nas versões de APIs não pagas, onde não podemos fazer muitas 
    requisições por segundo. 

    Args:
        amount_consumers (int): Quantidade de threads consumidoras definidas na função main de forma parametrizada 
        (neste caso = 1).
        c (objeto: cursor()): O cursor do BD criado na função main, pois passaremos por parâmetro ao criar a thread, 
        que vai realizar a escrita.
        connection (connect()): a conexão criada com o banco na função main()
    
    Returns:
        consumers (list): Lista de threads consumidoras
    """
    consumers = []
    
    consumer = consumerThread(amount_consumers, c, connection) 
    consumer.start()
    consumers.append(consumer)
    return consumers

class producerThread(threading.Thread): 
    """Thread produtora propriamente dita.

    Classe da Thread produtora, atributos e parâmetros descritos no método _init_.
    """
    def __init__(self, my_id, index, my_range, lat_lon):
        """Construtor da classe producerThread.

        Este método é o construtor da classe.
        
        Note:
            Os atributos possuem o mesmo nome dos argumentos.
            
        Args:
            my_id (int): ID da thread.
            index (int): Índice inicial do range de busca da lista lat_lon. Específico para cada thread.
            my_range (int): Range de busca de cada thread, representando o índice final. Específico para cada thread.
            lat_lon (list): Lista contendo todos pares de coordenadas a serem utilizados nas requisições
        """
        self.my_id = my_id
        self.index = index
        self.my_range = my_range
        self.lat_lon = lat_lon
        threading.Thread.__init__(self)
    def run(self):
        """Método que possui a real execução de cada thread.

        Define o que realmente cada thread irá executar. REALIZAÇÃO DA GEOCODIFICAÇÃO REVERSA. "location" recebe o objeto 
        Location contendo o endereço correspondente às coordenadas. Em seguida, a função getAddr vista anteriormente trata 
        o objeto location e retorna a lista contendo exatamente as informações a serem inseridas no banco de dados. 
        Por fim, a lista (addr) com as informações é adicionada em uma fila de endereços (q_addr - address queue)
        """
        for i in range(self.index, self.my_range): # range de busca específico para cada thread
            #geocodificação reversa
            location = reverse(self.lat_lon[i], exactly_one=True) # exactly_one=True retorna um formato com infos detalhadas
            addr = getAddr(location)
            q_addr.put(addr) 
            print('self.my_id: ', self.my_id)
            print('self.index: ', self.index)
            print('self.my_range: ', self.my_range)
            print('index atual: ', i)
            print('------------------------------')
#             time.sleep(1)

class consumerThread(threading.Thread):
    """Thread consumidora propriamente dita.

    Classe da Thread consumidora, atributos e parâmetros descritos no método _init_.
    """
    def __init__(self, my_id, c, connection):
        """Construtor da classe consumerThread.

        Este método é o construtor da classe.
        
        Note:
            Os atributos possuem o mesmo nome dos argumentos. Os que não possuirem estão comentados em linha
            
        Args:
            my_id (int): ID da thread.
            c (objeto cursor()): Cursor para o banco de dados.
            connection (connect()): a conexão criada com o banco na função main()
        """
        self.my_id = my_id
        self.c = c
        self.connection = connection
        self._running = True # Membro privado utilizado para controlar a execução da thread na main
        threading.Thread.__init__(self)
    def terminate(self):
        """Destrutor.

        Funciona como um "destrutor" da thread consumidora, adicionando o valor 'False' para _running
        """
        self._running = False
    def run(self):
        """Método que possui a real execução de cada thread.

        Define o que realmente cada thread irá executar. Enquanto _running for 'True' e ainda existir elementos na fila,
        um endereço é retirado da fila e a função 'dataentry' é chamada para adicionar o endereço no banco de dados.
        """
        while self._running:
            while q_addr.empty() == False:
                addr = q_addr.get()
                q_addr.task_done()
                dataentry(addr, self.c, self.connection)
                print('Escrevendo no banco......') 


# ### Função principal
#    Imporante lembrar que a API, na sua versão não paga, possui limitações, que se não seguidas podem resultar em exceções, erros ou até bloqueios. 
#    O GoogleV3 versão free permite até 50 requisiçoes por segundo, porém a chave de API (do candidato que vos fala) contida neste desafio é 
#    limitada também quanto ao número de requisições por mês e não suportará muitas execuções. É possível modificar a quantidade de threads produtoras
#	 a ser utilizada (em "amount_producers = ?), lembrando que não pode ultrapassar 50 req/s. Outra imporante exceção, diz respeito a quantidade de threads consumidoras. 
#    A aplicação só funcionará para UMA thread consumidora. Portanto, a quantidade de threads consumidoras deve ser 1. Isso porque mais threads consumidoras 
#    não aumentariam significativamente a velocidade da aplicação considerando a versão não paga, já que não podemos realizar muitas requisições ao mesmo tempo 
#    e o maior gargalo é na busca do endereço. Deste modo, acabei não desenvolvendo para mais de uma thread consumidora. Detalhes da função main por _docstrings_


def main():    
    """Função principal.

    Primeiramente é criada a fila "q_addr" que será compartilhada entre as threads produtoras e consumidoras. Logo depois, 
    em apenas uma linha é feita a conexão com banco de dados SQLite. Na própria conexão, o banco de dados já é criado, 
    caso o mesmo não exista. Após a conexão, é criado o cursor para a utilização dos comandos SQL e a função para criar 
    a tabela é chamada. Em seguida, é feita a leitura dos arquivos texto através da função read_file e cada par de 
    coordenada é adicionado como tupla na lista "lat_lon". Seguido, temos a parametrização da quantidade de threads a ser 
    utilizada. As threads são criadas e adicionadas em suas respectivas listas. Com Produtores e Consumidor já em 
    execução, a rotina espera as threads produtoras terminarem seu trabalho, em seguida, é feito o mesmo com a thread 
    consumidora. Para finalizar, a tela mostra que as threads terminaram seu trabalho e a conexão com bando de dados é 
    finalizada.
    
    Note:
        O uso de variáveis globais foi evitado durante o desafio, porém, a fila "q_addr" foi utilizada globalmente para 
        que as threads produtoras e consumidoras pudessem fazer uso. 
        APENAS a parametrização da quantidade de threads PRODUTORAS foi desenvolvida. Como dito anteriomente, o uso de 
        mais de uma thread consumidora não traria ganhos de performance significativo utilizando as versões free da API testada.
    """
    global q_addr # Fila utilizada nas threads produtoras e consumidora
    q_addr = queue.Queue()
    
    connection = sqlite3.connect('challenge_db.db', check_same_thread=False) # Cria e faz a conexão com o banco de dados
    c = connection.cursor() # cursor para utilizar comandos sql
    
    create_table(c) # cria a tabela indicada no desafio
    
    # Faz a leitura dos arquivos texto. 'data_points_teste.txt' foi criado para testes, com menos coordenadas
    # lat_lon = read_file('data_points_teste.txt')
    lat_lon = read_file('data_points_20180101.txt')
    lat_lon += read_file('data_points_20180102.txt')
    lat_lon += read_file('data_points_20180103.txt')

    print("Quantidade de requisições:", len(lat_lon), "\n") # Printa a quantidade de requisições que serão feitas
#     print(lat_lon)

    # Escolha da quantidade de threads produtoras
    amount_producers = 10; # Parametrizado (aconselhavel manter < 50 req/s)
    amount_consumers = 1; # Parametrização não desenvolvida, manter em 1
    
    producers = callProducers(amount_producers, lat_lon) # começa a execução das threads produtoras
    consumers = callConsumers(amount_consumers, c, connection) # começa a execução da thread consumidora
    
    for thread in producers:
        thread.join() # espera até que as threads produtoras terminem a execução
    
    for thread in consumers:
        thread.terminate() # sinaliza que as threads produtoras terminaram e a consumidora já pode finalizar    
        thread.join() # espera até que o resto dos endereços seja escrito no banco para finalizar
    
    for thread in producers:
        print("Thread ID: ", thread.my_id, "Em execução? ", thread.isAlive()) # mostra que as threads produtoras finalizaram
    print("\n")
    for thread in consumers:
        print("Thread ID: ", thread.my_id, "Em execução? ", thread.isAlive())# mostra que a thread consumidora finalizou
    
    connection.close() #fecha a conexão com o banco


# ### Chama a função principal

if __name__ == '__main__': #chamada da funcao principal
    main() #chamada da função main

